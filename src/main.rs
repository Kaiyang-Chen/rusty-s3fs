mod s3fs;
use fuser;
use clap::{crate_version, Arg, Command};
use std::env;
use std::io::ErrorKind;
use fuser::MountOption;
use log::error;
use crate::s3fs::S3FS;



fn main() {
    let matches = Command::new("S3-Fuse")
        .version(crate_version!())
        .author("Kaiyang Chen")
        .arg(
            Arg::new("data-dir")
                .long("data-dir")
                .value_name("DIR")
                .default_value("/tmp/fuser")
                .help("Set local directory used to store data")
                .takes_value(true),
        )
        .arg(
            Arg::new("mount-point")
                .long("mount-point")
                .short('m')
                .value_name("MOUNT_POINT")
                .required(true)
                .help("Act as a client, and mount FUSE at given path"),
        )
        .arg(
            Arg::new("auto_unmount")
                .long("auto_unmount")
                .help("Automatically unmount on process exit"),
        )
        .arg(
            Arg::new("allow-root")
                .long("allow-root")
                .help("Allow root user to access filesystem"),
        )
        .arg(
            Arg::new("direct-io")
                .long("direct-io")
                .requires("mount-point")
                .help("Mount FUSE with direct IO"),
        )
        .get_matches();
    env_logger::init();
    let mountpoint: String = matches
        .value_of("mount-point")
        .unwrap_or_default()
        .to_string();
    let mut options = vec![MountOption::RW, MountOption::FSName("s3-fuse".to_string())];
    if let Ok(enabled) = S3FS::fuse_allow_other_enabled() {
        if enabled {
            options.push(MountOption::AllowOther);
        }
    } else {
        eprintln!("Unable to read /etc/fuse.conf");
    };
    if matches.is_present("auto_unmount") {
        options.push(MountOption::AutoUnmount);
    }
    if matches.is_present("allow-root") {
        options.push(MountOption::AllowRoot);
    }
    let data_dir: String = matches.value_of("data-dir").unwrap_or_default().to_string();
    let result = fuser::mount2(
        S3FS::new(
            data_dir,
            matches.is_present("direct-io"),
        ),
        mountpoint,
        &options,
    );
    if let Err(e) = result {
        // Return a special error code for permission denied, which usually indicates that
        // "user_allow_other" is missing from /etc/fuse.conf
        if e.kind() == ErrorKind::PermissionDenied {
            error!("{}", e.to_string());
            std::process::exit(2);
        }
    }

}