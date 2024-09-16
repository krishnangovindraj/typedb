/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, fs, io, path::PathBuf};
use std::sync::Arc;

use database::{database_manager::DatabaseManager, DatabaseOpenError};
use database::transaction::{TransactionRead, TransactionSchema};
use options::TransactionOptions;
use query::query_manager::QueryManager;

use crate::{parameters::config::Config, service::typedb_service::TypeDBService};

#[derive(Debug)]
pub struct Server {
    data_directory: PathBuf,
    typedb_service: Option<TypeDBService>,
}

impl Server {
    pub fn open(config: Config) -> Result<Self, ServerOpenError> {
        use ServerOpenError::{CouldNotCreateDataDirectory, NotADirectory};
        let storage_directory = &config.storage.data;

        if !storage_directory.exists() {
            fs::create_dir_all(storage_directory)
                .map_err(|error| CouldNotCreateDataDirectory { path: storage_directory.to_owned(), source: error })?;
        } else if !storage_directory.is_dir() {
            return Err(NotADirectory { path: storage_directory.to_owned() });
        }

        let database_manager = DatabaseManager::new(storage_directory)
            .map_err(|err| ServerOpenError::DatabaseOpenError { source: err })?;
        let data_directory = storage_directory.to_owned();

        let typedb_service = TypeDBService::new(&config.server.address, database_manager);

        Ok(Self { data_directory, typedb_service: Some(typedb_service) })
    }

    pub fn database_manager(&self) -> &DatabaseManager {
        self.typedb_service.as_ref().unwrap().database_manager()
    }

    pub async fn serve(self) -> Result<(), Box<dyn Error>> {
        // let address = "localhost:1729".parse().unwrap();

        // TODO: could also construct in Server and await here only
        // Server::builder()
        // .http2_keepalive_interval()
        // .add_service(self.typedb_service.take().unwrap())
        // .serve(address)
        // .await?;
        // Ok(())
        self.TMP__distribution_test();
        Ok(())
    }

    fn TMP__distribution_test(&self) {
        println!("TODO: This currently runs a test which commits a simple schema, for testing. Remove it.");
        let dbm = self.typedb_service.as_ref().unwrap().database_manager() else {unreachable!()};
        dbm.create_database("tmp__distribution_test");
        let database = dbm.database("tmp__distribution_test").unwrap();
        let mut tx = TransactionSchema::open(database.clone(), TransactionOptions::default());
        let qm = QueryManager::new();
        qm.execute_schema(
            Arc::get_mut(&mut tx.snapshot).unwrap(),
            &tx.type_manager,
            &tx.thing_manager,
            typeql::parse_query("define attribute name @independent, value string;").unwrap().into_schema()
        ).unwrap();
        tx.commit().unwrap();
        println!("The schema has committed. Great success!");
    }
}

#[derive(Debug)]
pub enum ServerOpenError {
    NotADirectory { path: PathBuf },
    CouldNotCreateDataDirectory { path: PathBuf, source: io::Error },
    DatabaseOpenError { source: DatabaseOpenError },
}

impl Error for ServerOpenError {}

impl fmt::Display for ServerOpenError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
