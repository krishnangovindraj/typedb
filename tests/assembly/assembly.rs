/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::process::{Child, Command, Output};

fn build_cmd(cmd_str: &str) -> Command {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(cmd_str);
    cmd
}

fn kill_process(process: Child) -> std::io::Result<Output> {
    let mut process = process;
    match process.try_wait() {
        Ok(Some(_)) => {}
        _ => {
            let output = build_cmd(format!("kill -s TERM {}", process.id()).as_str())
                .output().expect("Failed to run kill command");
            if !output.status.success() {
                println!("kill-ing process failed: {:?}", output);
            }
        }
    }
    process.wait_with_output()
}

#[test]
fn test_assembly() {
    let tgz_path = "typedb-all-linux-x86_64.tar.gz";
    let tar_output = build_cmd(format!("tar -xf {tgz_path}").as_str())
        .output().expect("Failed to run tar");
    if !tar_output.status.success() {
        panic!("{:?}", tar_output);
    }
    let mut server_process = build_cmd("typedb-all-linux-x86_64/typedb_server_bin").spawn().expect("Failed to spawn server process");
    let test_result = run_test_against_server();
    let server_process_output = kill_process(server_process);

    if test_result.is_err() {
        println!("Server process output:\n{:?}", server_process_output);
        test_result.unwrap();
    }
}

fn run_test_against_server() -> Result<(),()> {
    // TODO
    Ok(())
}
