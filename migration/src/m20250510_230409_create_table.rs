use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Replace the sample below with your own migration scripts

        manager
            .create_table(
                Table::create()
                    .table(Swarm::Table)
                    .if_not_exists()
                    .col(pk_auto(Swarm::Id))
                    .col(binary_len(Swarm::InfoHash, 20))
                    .col(timestamp(Swarm::LastUpdated))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx-swarm-info_hash")
                    .table(Swarm::Table)
                    .col(Swarm::InfoHash)
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Peer::Table)
                    .if_not_exists()
                    .col(pk_auto(Peer::Id))
                    .col(unsigned(Peer::IpAddr))
                    .col(small_unsigned(Peer::Port))
                    .col(integer(Peer::Swarm))
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk-swarm")
                            .from(Peer::Table, Peer::Swarm)
                            .to(Swarm::Table, Swarm::Id)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .table(Peer::Table)
                    .name("idx-peer-swarm")
                    .col(Peer::Swarm)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Replace the sample below with your own migration scripts

        manager.drop_table(Table::drop().table(Swarm::Table).to_owned()).await?;
        manager
            .drop_index(Index::drop().name("idx-swarm-info_hash").to_owned())
            .await?;

        manager.drop_table(Table::drop().table(Peer::Table).to_owned()).await?;
        manager
            .drop_index(Index::drop().name("idx-peer-swarm").to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum Swarm {
    Table,
    Id,
    InfoHash,
    LastUpdated,
}

#[derive(DeriveIden)]
enum Peer {
    Table,
    Id,
    Swarm,
    IpAddr,
    Port,
}
