import { DeduplicateJoinsPlugin, ExpressionBuilder, Kysely, SelectQueryBuilder, sql } from 'kysely';
import { jsonArrayFrom, jsonObjectFrom } from 'kysely/helpers/postgres';
import { AssetSearchBuilderOptions } from 'src/interfaces/search.interface';
import { Assets, DB } from 'src/prisma/generated/types';
import { Between, LessThanOrEqual, MoreThanOrEqual } from 'typeorm';

/**
 * Allows optional values unlike the regular Between and uses MoreThanOrEqual
 * or LessThanOrEqual when only one parameter is specified.
 */
export function OptionalBetween<T>(from?: T, to?: T) {
  if (from && to) {
    return Between(from, to);
  } else if (from) {
    return MoreThanOrEqual(from);
  } else if (to) {
    return LessThanOrEqual(to);
  }
}

export const getUpsertColumns = async (tableName: string, pk: string, db: Kysely<DB>) => {
  const tables = await db.introspection.getTables();
  const table = tables.find((table) => table.name === tableName)!;
  return Object.fromEntries(
    table.columns
      .map((column) => column.name)
      .filter((column) => column !== pk)
      .map((column) => [column, sql`excluded.${sql.ref(column)}`]),
  );
};

export const mapUpsertColumns = (columns: Record<string, any>, entry: Record<string, any>) => {
  const obj: Record<string, any> = {};
  for (const key of Object.keys(entry)) {
    obj[key] = columns[key];
  }

  return obj;
};

export const withExif = <O>(qb: SelectQueryBuilder<DB, 'assets', O>) =>
  qb
    .leftJoin('exif', 'assets.id', 'exif.assetId')
    .select((eb) => eb.fn('jsonb_strip_nulls', [eb.fn('to_jsonb', [eb.table('exif')])]).as('exifInfo'));

export const withSmartSearch = <O>(qb: SelectQueryBuilder<DB, 'assets', O>, { inner }: { inner: boolean }) => {
  const join = inner ? qb.innerJoin : qb.leftJoin;
  return join('smart_search', 'smart_search.assetId', 'assets.id').select('smart_search.embedding' as any);
};

export const withFaces = (eb: ExpressionBuilder<DB, 'assets'>) =>
  jsonArrayFrom(eb.selectFrom('asset_faces').selectAll().whereRef('asset_faces.assetId', '=', 'assets.id')).as('faces');

export const withFacesAndPeople = (eb: ExpressionBuilder<DB, 'assets'>) =>
  eb
    .selectFrom('asset_faces')
    .leftJoin('person', 'person.id', 'asset_faces.personId')
    .whereRef('asset_faces.assetId', '=', 'assets.id')
    .select((eb) =>
      eb
        .fn('jsonb_agg', [
          eb
            .case()
            .when('person.id', 'is not', null)
            .then(
              eb.fn('jsonb_insert', [
                eb.fn('to_jsonb', [eb.table('asset_faces')]),
                sql`'{person}'::text[]`,
                eb.fn('to_jsonb', [eb.table('person')]),
              ]),
            )
            .else(eb.fn('to_jsonb', [eb.table('asset_faces')]))
            .end(),
        ])
        .as('faces'),
    )
    .as('faces');

export const hasPeopleCte = (db: Kysely<DB>, personIds: string[]) =>
  db.with('valid', (qb) =>
    qb
      .selectFrom('asset_faces')
      .select('assetId')
      .where('personId', '=', anyUuid(personIds!))
      .groupBy('assetId')
      .having((eb) => eb.fn.count('personId'), '>=', personIds.length),
  );

export const hasPeople = (db: Kysely<DB>, personIds?: string[]) =>
  personIds && personIds.length > 0
    ? hasPeopleCte(db, personIds).selectFrom('assets').innerJoin('valid', 'valid.assetId', 'assets.id')
    : db.selectFrom('assets');

export const withOwner = (eb: ExpressionBuilder<DB, 'assets'>) =>
  jsonObjectFrom(eb.selectFrom('users').selectAll().whereRef('users.id', '=', 'assets.ownerId')).as('owner');

export const withLibrary = (eb: ExpressionBuilder<DB, 'assets'>) =>
  jsonObjectFrom(eb.selectFrom('libraries').selectAll().whereRef('libraries.id', '=', 'assets.libraryId')).as(
    'library',
  );

export const withStack = <O>(
  qb: SelectQueryBuilder<DB, 'assets', O>,
  { assets, withDeleted }: { assets: boolean; withDeleted?: boolean },
) =>
  qb
    .leftJoin('asset_stack', 'asset_stack.primaryAssetId', 'assets.id')
    .select((eb) => eb.fn.toJson('asset_stack').as('stack'))
    .where((eb) =>
      eb.or([eb('asset_stack.primaryAssetId', '=', eb.ref('assets.id')), eb('assets.stackId', 'is', null)]),
    )
    .$if(assets, (qb) =>
      qb.select((eb) =>
        eb
          .selectFrom('assets as stacked')
          .select(sql<Assets[]>`json_agg(jsonb_strip_nulls(to_jsonb(stacked)))`.as('stackedAssets'))
          .whereRef('asset_stack.id', '=', 'assets.stackId')
          .whereRef('asset_stack.primaryAssetId', '!=', 'assets.id')
          .$if(!withDeleted, (qb) => qb.where('stacked.deletedAt', 'is', null))
          .as('stackedAssets'),
      ),
    );

export const withAlbums = <O>(qb: SelectQueryBuilder<DB, 'assets', O>, { albumId }: { albumId?: string }) => {
  return qb
    .select((eb) =>
      jsonArrayFrom(
        eb
          .selectFrom('albums')
          .selectAll()
          .innerJoin('albums_assets_assets', (join) =>
            join
              .onRef('albums.id', '=', 'albums_assets_assets.albumsId')
              .onRef('assets.id', '=', 'albums_assets_assets.assetsId'),
          )
          .whereRef('albums.id', '=', 'albums_assets_assets.albumsId')
          .$if(!!albumId, (qb) => qb.where('albums.id', '=', asUuid(albumId!))),
      ).as('albums'),
    )
    .$if(!!albumId, (qb) =>
      qb.where((eb) =>
        eb.exists((eb) =>
          eb
            .selectFrom('albums_assets_assets')
            .whereRef('albums_assets_assets.assetsId', '=', 'assets.id')
            .where('albums_assets_assets.albumsId', '=', asUuid(albumId!)),
        ),
      ),
    );
};

export const asUuid = (id: string) => sql<string>`${id}::uuid`;

export const anyUuid = (ids: string[]) => sql<string>`any(${`{${ids}}`}::uuid[])`;

export const asVector = (embedding: number[]) => sql<number[]>`${`[${embedding}]`}::vector`;

const joinDeduplicationPlugin = new DeduplicateJoinsPlugin();

export function searchAssetBuilder(kysely: Kysely<DB>, options: AssetSearchBuilderOptions) {
  options.isArchived ??= options.withArchived ? undefined : false;
  options.withDeleted ||= !!(options.trashedAfter || options.trashedBefore);
  return hasPeople(kysely.withPlugin(joinDeduplicationPlugin), options.personIds)
    .selectAll('assets')
    .$if(!!options.createdBefore, (qb) => qb.where('assets.createdAt', '<=', options.createdBefore!))
    .$if(!!options.createdAfter, (qb) => qb.where('assets.createdAt', '>=', options.createdAfter!))
    .$if(!!options.updatedBefore, (qb) => qb.where('assets.updatedAt', '<=', options.updatedBefore!))
    .$if(!!options.updatedAfter, (qb) => qb.where('assets.updatedAt', '>=', options.updatedAfter!))
    .$if(!!options.trashedBefore, (qb) => qb.where('assets.deletedAt', '<=', options.trashedBefore!))
    .$if(!!options.trashedAfter, (qb) => qb.where('assets.deletedAt', '>=', options.trashedAfter!))
    .$if(!!options.takenBefore, (qb) => qb.where('assets.fileCreatedAt', '<=', options.takenBefore!))
    .$if(!!options.takenAfter, (qb) => qb.where('assets.fileCreatedAt', '>=', options.takenAfter!))
    .$if(!!options.city, (qb) =>
      qb.leftJoin('exif', 'assets.id', 'exif.assetId').where('exif.city', '=', options.city!),
    )
    .$if(!!options.country, (qb) =>
      qb.leftJoin('exif', 'assets.id', 'exif.assetId').where('exif.country', '=', options.country!),
    )
    .$if(!!options.lensModel, (qb) =>
      qb.leftJoin('exif', 'assets.id', 'exif.assetId').where('exif.lensModel', '=', options.lensModel!),
    )
    .$if(!!options.make, (qb) =>
      qb.leftJoin('exif', 'assets.id', 'exif.assetId').where('exif.make', '=', options.make!),
    )
    .$if(!!options.model, (qb) =>
      qb.leftJoin('exif', 'assets.id', 'exif.assetId').where('exif.model', '=', options.model!),
    )
    .$if(!!options.state, (qb) =>
      qb.leftJoin('exif', 'assets.id', 'exif.assetId').where('exif.state', '=', options.state!),
    )
    .$if(!!options.checksum, (qb) => qb.where('assets.checksum', '=', options.checksum!))
    .$if(!!options.deviceAssetId, (qb) => qb.where('assets.deviceAssetId', '=', asUuid(options.deviceAssetId!)))
    .$if(!!options.deviceId, (qb) => qb.where('assets.deviceId', '=', asUuid(options.deviceId!)))
    .$if(!!options.id, (qb) => qb.where('assets.id', '=', asUuid(options.id!)))
    .$if(!!options.libraryId, (qb) => qb.where('assets.libraryId', '=', asUuid(options.libraryId!)))
    .$if(!!options.userIds, (qb) => qb.where('assets.ownerId', '=', anyUuid(options.userIds!)))
    .$if(!!options.encodedVideoPath, (qb) => qb.where('assets.encodedVideoPath', '=', options.encodedVideoPath!))
    .$if(!!options.originalPath, (qb) => qb.where('assets.originalPath', '=', options.originalPath!))
    .$if(!!options.previewPath, (qb) => qb.where('assets.previewPath', '=', options.previewPath!))
    .$if(!!options.thumbnailPath, (qb) => qb.where('assets.thumbnailPath', '=', options.thumbnailPath!))
    .$if(!!options.originalFileName, (qb) =>
      qb.where(sql`f_unaccent(assets.originalFileName)`, 'ilike', sql`f_unaccent(${options.originalFileName})`),
    )
    .$if(!!options.isFavorite, (qb) => qb.where('assets.isFavorite', '=', options.isFavorite!))
    .$if(!!options.isOffline, (qb) => qb.where('assets.isOffline', '=', options.isOffline!))
    .$if(!!options.isVisible, (qb) => qb.where('assets.isVisible', '=', options.isVisible!))
    .$if(!!options.type, (qb) => qb.where('assets.type', '=', options.type!))
    .$if(!!options.isArchived, (qb) => qb.where('assets.isArchived', '=', options.isArchived!))
    .$if(!!options.isEncoded, (qb) => qb.where('assets.encodedVideoPath', 'is not', null))
    .$if(!!options.isMotion, (qb) => qb.where('assets.livePhotoVideoId', 'is not', null))
    .$if(!!options.isNotInAlbum, (qb) =>
      qb.where((eb) =>
        eb.not(eb.exists((eb) => eb.selectFrom('albums_assets_assets').where('assetsId', '=', 'assets.id'))),
      ),
    )
    .$if(!!options.withExif, (qb) => withExif(qb))
    .$if(!!(options.withFaces || options.withPeople || options.personIds), (qb) =>
      qb.select((eb) => withFacesAndPeople(eb)),
    )
    .$if(!options.withDeleted, (qb) => qb.where('assets.deletedAt', 'is', null));
}
