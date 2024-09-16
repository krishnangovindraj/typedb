/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::io;
use std::process::{Command, Output};
use std::thread::sleep;
use std::time::Duration;

fn build_cmd(cmd_str: &str) -> Command {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(cmd_str);
    cmd
}

#[test]
fn test_assembly() {
    let tgz_path = "typedb-all-linux-x86_64.tar.gz";
    let tar_output = build_cmd(format!("tar -xf {tgz_path}").as_str()).output().expect("Failed to run tar");
    if !tar_output.status.success() {
        panic!("{:?}", tar_output);
    }
    let mut server_process = build_cmd("typedb-all-linux-x86_64/typedb_server_bin").spawn().expect("Failed to spawn server process");
    let test_result = run_test_against_server();
    build_cmd(format!("kill -s TERM {}", server_process.id()).as_str()).output().expect("kill-ing server failed");
    let output = server_process.wait_with_output();
    if test_result.is_err() {
        println!("Server process output:\n{:?}", output);
        test_result.unwrap();
    }
}

fn run_test_against_server() -> Result<(),()> {
    // TODO
    Ok(())
}
