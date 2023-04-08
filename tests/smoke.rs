use std::process::Command;
use std::time::Duration;
use std::{fs, io};

use anyhow::Result;

#[test]
fn smoke() -> Result<()> {
    let home = tempfile::tempdir()?;
    let mut app = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_batchy"))
            .current_dir(home.path())
            .spawn()?,
    );
    let mut tries = 10;
    loop {
        if let Some(resp) = ureq::get("http://localhost:3000/healthcheck").call().ok() {
            assert_eq!(resp.status(), 200);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
        if tries == 0 {
            panic!("unable to healthcheck (start?)");
        }
        tries -= 1;
    }
    ureq::post("http://localhost:3000/store").send_string("hello world")?;
    ureq::post("http://localhost:3000/store").send_string("goodbye world")?;

    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(app.0.id().try_into()?),
        nix::sys::signal::Signal::SIGTERM,
    )?;

    assert!(app.0.wait()?.success());
    let mut items = Vec::new();

    for entry in fs::read_dir(home.path())? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let name = entry.file_name();
            let name = name.to_str().unwrap();
            if !name.ends_with(".events.archiv") {
                continue;
            }
            let opts = archiv::ExpandOptions::default();
            let mut archiv = opts.stream(io::BufReader::new(fs::File::open(entry.path())?))?;
            while let Some(mut item) = archiv.next_item()? {
                let mut s = Vec::new();
                item.read_to_end(&mut s)?;
                items.push(String::from_utf8(s[8..].to_vec())?);
            }
        }
    }

    assert_eq!(items, vec!["hello world", "goodbye world"]);

    Ok(())
}

struct KillOnDrop(std::process::Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}
