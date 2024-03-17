import { Inject, Injectable } from '@nestjs/common';
import { DeduplicateJoinsPlugin, Kysely, OrderByDirectionExpression, sql } from 'kysely';
import { InjectKysely } from 'nestjs-kysely';
import { DummyValue, GenerateSql } from 'src/decorators';
import { AssetFaceEntity } from 'src/entities/asset-face.entity';
import { AssetEntity } from 'src/entities/asset.entity';
import { GeodataPlacesEntity } from 'src/entities/geodata-places.entity';
import { ILoggerRepository } from 'src/interfaces/logger.interface';
import {
  AssetDuplicateResult,
  AssetDuplicateSearch,
  AssetSearchOptions,
  FaceEmbeddingSearch,
  FaceSearchResult,
  ISearchRepository,
  SearchPaginationOptions,
  SmartSearchOptions,
} from 'src/interfaces/search.interface';
import { DB } from 'src/prisma/generated/types';
import { anyUuid, asUuid, asVector, searchAssetBuilder } from 'src/utils/database';
import { Instrumentation } from 'src/utils/instrumentation';
import { Paginated } from 'src/utils/pagination';
import { isValidInteger } from 'src/validation';

@Instrumentation()
@Injectable()
export class SearchRepository implements ISearchRepository {
  joinDeduplicationPlugin = new DeduplicateJoinsPlugin();
  constructor(
    @Inject(ILoggerRepository) private logger: ILoggerRepository,
    @InjectKysely() private db: Kysely<DB>,
  ) {
    this.logger.setContext(SearchRepository.name);
  }

  @GenerateSql({
    params: [
      { page: 1, size: 100 },
      {
        takenAfter: DummyValue.DATE,
        lensModel: DummyValue.STRING,
        ownerId: DummyValue.UUID,
        withStacked: true,
        isFavorite: true,
        ownerIds: [DummyValue.UUID],
      },
    ],
  })
  async searchMetadata(pagination: SearchPaginationOptions, options: AssetSearchOptions): Paginated<AssetEntity> {
    const orderDirection = (options.orderDirection?.toLowerCase() || 'desc') as OrderByDirectionExpression;
    const items = await searchAssetBuilder(this.db, options)
      .orderBy('assets.fileCreatedAt', orderDirection)
      .limit(pagination.size + 1)
      .offset((pagination.page - 1) * pagination.size)
      .execute();
    const hasNextPage = items.length > pagination.size;
    items.splice(pagination.size);
    return { items: items as any as AssetEntity[], hasNextPage };
  }

  @GenerateSql({
    params: [
      { page: 1, size: 100 },
      {
        takenAfter: DummyValue.DATE,
        embedding: Array.from({ length: 512 }, Math.random),
        lensModel: DummyValue.STRING,
        withStacked: true,
        isFavorite: true,
        userIds: [DummyValue.UUID],
      },
    ],
  })
  async searchSmart(pagination: SearchPaginationOptions, options: SmartSearchOptions): Paginated<AssetEntity> {
    if (!isValidInteger(pagination.size, { min: 1, max: 1000 })) {
      throw new Error(`Invalid value for 'size': ${pagination.size}`);
    }

    let items: AssetEntity[] = [];
    const builder = searchAssetBuilder(this.db, options)
      .innerJoin('smart_search', 'assets.id', 'smart_search.assetId')
      .orderBy(sql`smart_search.embedding <=> ${asVector(options.embedding)}::vector`)
      .limit(pagination.size + 1)
      .offset((pagination.page - 1) * pagination.size);
    console.log(builder.compile());

    items = (await builder.execute()) as any as AssetEntity[];

    const hasNextPage = items.length > pagination.size;
    items.splice(pagination.size);
    return { items, hasNextPage };
  }

  @GenerateSql({
    params: [
      {
        embedding: Array.from({ length: 512 }, Math.random),
        maxDistance: 0.6,
        userIds: [DummyValue.UUID],
      },
    ],
  })
  searchDuplicates({
    assetId,
    embedding,
    maxDistance,
    type,
    userIds,
  }: AssetDuplicateSearch): Promise<AssetDuplicateResult[]> {
    const vector = asVector(embedding);
    return this.db
      .with('cte', (qb) =>
        qb
          .selectFrom('assets')
          .select([
            'assets.id as assetId',
            'assets.duplicateId',
            sql<number>`smart_search.embedding <=> ${vector}::vector`.as('distance'),
          ])
          .innerJoin('smart_search', 'assets.id', 'smart_search.assetId')
          .where('assets.ownerId', '=', anyUuid(userIds))
          .where('assets.isVisible', '=', true)
          .where('assets.type', '=', type)
          .where('assets.id', '!=', assetId)
          .orderBy(sql`smart_search.embedding <=> ${vector}::vector`)
          .limit(64),
      )
      .selectFrom('cte')
      .selectAll()
      .where('cte.distance', '<=', maxDistance as number)
      .execute();
  }

  @GenerateSql({
    params: [
      {
        userIds: [DummyValue.UUID],
        embedding: Array.from({ length: 512 }, Math.random),
        numResults: 100,
        maxDistance: 0.6,
      },
    ],
  })
  searchFaces({
    userIds,
    embedding,
    numResults,
    maxDistance,
    hasPerson,
  }: FaceEmbeddingSearch): Promise<FaceSearchResult[]> {
    if (!isValidInteger(numResults, { min: 1, max: 1000 })) {
      throw new Error(`Invalid value for 'numResults': ${numResults}`);
    }

    // setting this too low messes with prefilter recall
    numResults = Math.max(numResults, 64);
    const vector = asVector(embedding);
    return this.db.transaction().execute(async (tx) => {
      await sql.raw(`SET LOCAL vectors.hnsw_ef_search = ${numResults}`).execute(tx);
      return tx
        .with('cte', (qb) =>
          qb
            .selectFrom('asset_faces')
            .select([
              (eb) => eb.fn.toJson(sql`asset_faces.*`).as('face'),
              sql<number>`asset_faces.embedding <=> ${vector}::vector`.as('distance'),
            ])
            .innerJoin('assets', 'assets.id', 'asset_faces.assetId')
            .where('assets.ownerId', '=', anyUuid(userIds))
            .$if(!!hasPerson, (qb) => qb.where('asset_faces.personId', 'is not', null))
            .orderBy(sql`asset_faces.embedding <=> ${vector}::vector`)
            .limit(numResults),
        )
        .selectFrom('cte')
        .selectAll()
        .where('cte.distance', '<=', maxDistance)
        .execute() as any as Array<{ face: AssetFaceEntity; distance: number }>;
    });
  }

  @GenerateSql({ params: [DummyValue.STRING] })
  searchPlaces(placeName: string): Promise<GeodataPlacesEntity[]> {
    const contains = '%>>' as any as 'ilike';
    return this.db
      .selectFrom('geodata_places')
      .selectAll()
      .where((eb) =>
        eb.or([
          eb(eb.fn('f_unaccent', ['name']), contains, eb.fn('f_unaccent', [eb.val(placeName)])),
          eb(eb.fn('f_unaccent', ['admin2Name']), contains, eb.fn('f_unaccent', [eb.val(placeName)])),
          eb(eb.fn('f_unaccent', ['admin1Name']), contains, eb.fn('f_unaccent', [eb.val(placeName)])),
          eb(eb.fn('f_unaccent', ['alternateNames']), contains, eb.fn('f_unaccent', [eb.val(placeName)])),
        ]),
      )
      .orderBy(
        sql`
          COALESCE(f_unaccent(name) <->>> f_unaccent(${placeName}), 0.1) +
          COALESCE(f_unaccent("admin2Name") <->>> f_unaccent(${placeName}), 0.1) +
          COALESCE(f_unaccent("admin1Name") <->>> f_unaccent(${placeName}), 0.1) +
          COALESCE(f_unaccent("alternateNames") <->>> f_unaccent(${placeName}), 0.1)
        `,
      )
      .limit(20)
      .execute() as Promise<GeodataPlacesEntity[]>;
  }

  @GenerateSql({ params: [[DummyValue.UUID]] })
  getAssetsByCity(userIds: string[]): Promise<AssetEntity[]> {
    return this.db
      .withRecursive('cte', (qb) => {
        const base = qb
          .selectFrom('exif')
          .select(['city', 'assetId'])
          .innerJoin('assets', 'assets.id', 'exif.assetId')
          .where('assets.ownerId', '=', anyUuid(userIds))
          .where('assets.isVisible', '=', true)
          .where('assets.isArchived', '=', false)
          .where('assets.type', '=', 'IMAGE')
          .orderBy('city')
          .limit(1);

        const recursive = qb
          .selectFrom('cte')
          .select(['l.city', 'l.assetId'])
          .innerJoinLateral(
            (qb) =>
              qb
                .selectFrom('exif')
                .select(['city', 'assetId'])
                .innerJoin('assets', 'assets.id', 'exif.assetId')
                .where('assets.ownerId', '=', anyUuid(userIds))
                .where('assets.isVisible', '=', true)
                .where('assets.isArchived', '=', false)
                .where('assets.type', '=', 'IMAGE')
                .whereRef('exif.city', '>', 'cte.city')
                .orderBy('city')
                .limit(1)
                .as('l'),
            (join) => join.onTrue(),
          );

        return sql<{ city: string; assetId: string }>`(${base} union all ${recursive})`;
      })
      .selectFrom('assets')
      .innerJoin('exif', 'assets.id', 'exif.assetId')
      .innerJoin('cte', 'assets.id', 'cte.assetId')
      .selectAll('assets')
      .select((eb) => eb.fn('jsonb_strip_nulls', [eb.fn('to_jsonb', [eb.table('exif')])]).as('exifInfo'))
      .orderBy('exif.city')
      .execute() as any as Promise<AssetEntity[]>;
  }

  async upsert(assetId: string, embedding: number[]): Promise<void> {
    const vector = asVector(embedding);
    await this.db
      .insertInto('smart_search')
      .values({ assetId: asUuid(assetId), embedding: vector } as any)
      .onConflict((oc) => oc.column('assetId').doUpdateSet({ embedding: vector } as any))
      .execute();
  }

  async getDimensionSize(): Promise<number> {
    const { rows } = await sql<{ dimsize: number }>`
      SELECT atttypmod as dimsize
      FROM pg_attribute f
        JOIN pg_class c ON c.oid = f.attrelid
      WHERE c.relkind = 'r'::char
        AND f.attnum > 0
        AND c.relname = 'smart_search'
        AND f.attname = 'embedding'
    `.execute(this.db);

    const dimSize = rows[0]['dimsize'];
    if (!isValidInteger(dimSize, { min: 1, max: 2 ** 16 })) {
      throw new Error(`Could not retrieve CLIP dimension size`);
    }
    return dimSize;
  }

  setDimensionSize(dimSize: number): Promise<void> {
    if (!isValidInteger(dimSize, { min: 1, max: 2 ** 16 })) {
      throw new Error(`Invalid CLIP dimension size: ${dimSize}`);
    }

    return this.db.transaction().execute(async (trx) => {
      await sql`TRUNCATE smart_search`.execute(trx);
      await sql`ALTER TABLE smart_search ALTER COLUMN embedding SET DATA TYPE vector(${sql.lit(dimSize)})`.execute(trx);
      await sql`REINDEX INDEX clip_index`.execute(trx);
    });
  }

  async deleteAllSearchEmbeddings(): Promise<void> {
    await sql`TRUNCATE ${sql.table('smart_search')}`.execute(this.db);
  }
}
