import { MigrationInterface, QueryRunner } from "typeorm";

export class AddAssetFilesTable1723859965844 implements MigrationInterface {
    name = 'AddAssetFilesTable1723859965844'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "asset_files" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "assetId" uuid NOT NULL, "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "deletedAt" TIMESTAMP WITH TIME ZONE, "type" character varying NOT NULL, "path" character varying NOT NULL, CONSTRAINT "PK_c41dc3e9ef5e1c57ca5a08a0004" PRIMARY KEY ("id"))`);
        await queryRunner.query(`CREATE UNIQUE INDEX "UQ_assetId_type" ON "asset_files" ("assetId", "type") `);

        await queryRunner.query(`ALTER TABLE "asset_files" ADD CONSTRAINT "FK_e3e103a5f1d8bc8402999286040" FOREIGN KEY ("assetId") REFERENCES "assets"("id") ON DELETE CASCADE ON UPDATE CASCADE`);

        // preview path migration
        await queryRunner.query(`INSERT INTO "asset_files" ("assetId", "type", "path") SELECT "id", 'preview', "previewPath" FROM "assets" WHERE "previewPath" IS NOT NULL AND "previewPath" != ''`);
        await queryRunner.query(`ALTER TABLE "assets" DROP COLUMN "previewPath"`);

        // thumbnail path migration
        await queryRunner.query(`INSERT INTO "asset_files" ("assetId", "type", "path") SELECT "id", 'thumbnail', "thumbnailPath" FROM "assets" WHERE "thumbnailPath" IS NOT NULL AND "thumbnailPath" != ''`);
        await queryRunner.query(`ALTER TABLE "assets" DROP COLUMN "thumbnailPath"`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {

        // undo preview path migration
        await queryRunner.query(`ALTER TABLE "assets" ADD "previewPath" character varying`);
        await queryRunner.query(`UPDATE "assets" SET "previewPath" = "asset_files".path FROM "asset_files" WHERE "assets".id = "asset_files".assetId AND "asset_files".type = 'preview'`);

        // undo thumbnail path migration
        await queryRunner.query(`ALTER TABLE "assets" ADD "thumbnailPath" character varying DEFAULT ''`);
        await queryRunner.query(`UPDATE "assets" SET "thumbnailPath" = "asset_files".path FROM "asset_files" WHERE "assets".id = "asset_files".assetId AND "asset_files".type = 'thumbnail'`);

        await queryRunner.query(`ALTER TABLE "asset_files" DROP CONSTRAINT "FK_e3e103a5f1d8bc8402999286040"`);
        await queryRunner.query(`DROP TABLE "asset_files"`);
    }

}
