import { AssetEntity } from 'src/entities/asset.entity';
import { AssetFileType } from 'src/enum';
import {
  Column,
  CreateDateColumn,
  DeleteDateColumn,
  Entity,
  Index,
  ManyToOne,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

export const ASSET_CHECKSUM_CONSTRAINT = 'UQ_assets_owner_checksum';

@Index('UQ_assetId_type', ['assetId', 'type'], { unique: true })
@Entity('asset_files')
export class AssetFileEntity {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column()
  assetId!: string;

  @CreateDateColumn({ type: 'timestamptz' })
  createdAt!: Date;

  @UpdateDateColumn({ type: 'timestamptz' })
  updatedAt!: Date;

  @DeleteDateColumn({ type: 'timestamptz', nullable: true })
  deletedAt!: Date | null;

  @ManyToOne(() => AssetEntity, { onDelete: 'CASCADE', onUpdate: 'CASCADE' })
  asset?: AssetEntity;

  @Column()
  type!: AssetFileType;

  @Column()
  path!: string;
}
