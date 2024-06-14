use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use bpaf::*;
use frost::query::Query;
use itertools::Itertools;

use frost::errors::Error;
use frost::{BagMetadata, DecompressedBag};

#[derive(Clone, Debug)]
enum Opts {
    TopicOptions { file_path: PathBuf },
    TypeOptions { file_path: PathBuf },
    SizeOptions { use_types: bool, file_path: PathBuf },
    InfoOptions { minimal: bool, file_path: PathBuf },
}

fn file_parser() -> impl Parser<PathBuf> {
    positional::<PathBuf>("FILE").complete_shell(ShellComp::File { mask: None })
}

fn args() -> Opts {
    let file_path = file_parser();
    let minimal = short('m')
        .long("minimal")
        .help("Show minimal info (without types/topics)")
        .switch();
    let info_cmd = construct!(Opts::InfoOptions { minimal, file_path })
        .to_options()
        .descr("Print rosbag information")
        .command("info");
    let file_path = file_parser();
    let topics_cmd = construct!(Opts::TopicOptions { file_path })
        .to_options()
        .descr("Print rosbag topics")
        .command("topics");
    let file_path = file_parser();
    let types_cmd = construct!(Opts::TypeOptions { file_path })
        .to_options()
        .descr("Print rosbag types")
        .command("types");
    let file_path = file_parser();
    let use_types: parsers::ParseFlag<bool> = long("types")
        .help("Aggregate size information by type")
        .switch();
    let size_cmd = construct!(Opts::SizeOptions {
        use_types,
        file_path
    })
    .to_options()
    .descr("Print size info about topics or types with --types")
    .command("size");
    let parser = construct!([info_cmd, topics_cmd, types_cmd, size_cmd]);
    parser.to_options().version(env!("CARGO_PKG_VERSION")).run()
}

fn max_type_len(metadata: &BagMetadata) -> usize {
    metadata
        .connection_data
        .values()
        .map(|d| d.data_type.len())
        .max()
        .unwrap_or(0)
}

fn max_topic_len(metadata: &BagMetadata) -> usize {
    metadata
        .connection_data
        .values()
        .map(|d| d.topic.len())
        .max()
        .unwrap_or(0)
}

fn print_topics(metadata: &BagMetadata, writer: &mut impl Write) -> Result<(), Error> {
    for topic in metadata.topics().into_iter().sorted() {
        writer.write_all(format!("{topic}\n").as_bytes())?
    }
    Ok(())
}

fn print_types(metadata: &BagMetadata, writer: &mut impl Write) -> Result<(), Error> {
    for topic in metadata.types().into_iter().sorted() {
        writer.write_all(format!("{topic}\n").as_bytes())?
    }
    Ok(())
}

fn human_bytes(bytes: u64) -> String {
    let units = ["bytes", "KB", "MB", "GB"];

    let mut unit = units[0];
    let mut remainder = bytes as f64;

    for u in units {
        unit = u;
        if remainder < 1024.0 {
            break;
        }
        remainder /= 1024.0;
    }

    if unit == "bytes" {
        format!("{bytes} bytes")
    } else {
        format!("{remainder:.2} {unit} ({bytes} bytes)")
    }
}

fn print_all(metadata: &BagMetadata, minimal: bool, writer: &mut impl Write) -> Result<(), Error> {
    let start_time = metadata
        .start_time()
        .expect("Bag does not have a start time");
    let end_time = metadata.end_time().expect("Bag does not have a end time");

    writer.write_all(
        format!(
            "{0: <13}{1}\n",
            "path:",
            metadata
                .file_path
                .as_ref()
                .map_or_else(|| "None".to_string(), |p| p.to_string_lossy().into_owned())
        )
        .as_bytes(),
    )?;
    writer.write_all(format!("{0: <13}{1}\n", "version:", metadata.version).as_bytes())?;
    writer.write_all(
        format!(
            "{0: <13}{1:.2}s\n",
            "duration:",
            metadata.duration().as_secs()
        )
        .as_bytes(),
    )?;
    writer.write_all(
        format!(
            "{0: <13}{1} ({2:.6})\n",
            "start:",
            start_time.as_datetime().unwrap_or_default(),
            f64::from(start_time)
        )
        .as_bytes(),
    )?;
    writer.write_all(
        format!(
            "{0: <13}{1} ({2:.6})\n",
            "end:",
            end_time.as_datetime().unwrap_or_default(),
            f64::from(end_time)
        )
        .as_bytes(),
    )?;

    writer
        .write_all(format!("{0: <13}{1}\n", "size:", human_bytes(metadata.num_bytes)).as_bytes())?;

    writer.write_all(format!("{0: <13}{1}\n", "messages:", metadata.message_count()).as_bytes())?;

    let compression_info = metadata.compression_info();

    let total_chunks: usize = compression_info.iter().map(|info| info.chunk_count).sum();
    let max_compression_name = compression_info
        .iter()
        .map(|info| info.name.len())
        .max()
        .unwrap_or(0);
    for (i, info) in compression_info.iter().enumerate() {
        let col_display = if i == 0 { "compression:" } else { "" };
        writer.write_all(
            format!(
                "{0: <13}{1: <max_compression_name$} [{2}/{3} chunks; {4:.2}%]\n",
                col_display,
                info.name,
                info.chunk_count,
                total_chunks,
                (100f64 * info.total_compressed as f64 / info.total_uncompressed as f64)
            )
            .as_bytes(),
        )?;
    }

    if minimal {
        return Ok(());
    }

    let max_type_len: usize = max_type_len(metadata);
    for (i, (data_type, md5sum)) in metadata
        .connection_data
        .values()
        .map(|data| (data.data_type.clone(), data.md5sum.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .enumerate()
    {
        let col_display = if i == 0 { "types:" } else { "" };
        writer.write_all(
            format!(
                "{0: <13}{1: <max_type_len$} [{2}]\n",
                col_display, data_type, md5sum
            )
            .as_bytes(),
        )?;
    }

    let max_topic_len = max_topic_len(metadata);

    let topic_counts = metadata.topic_message_counts();

    for (i, (topic, data_type)) in metadata
        .topics_and_types()
        .into_iter()
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .enumerate()
    {
        let col_display = if i == 0 { "topics:" } else { "" };
        let msg_count = topic_counts.get(topic).unwrap_or(&0);
        writer.write_all(
            format!(
                "{0: <13}{1: <max_topic_len$} {2:>10} msgs : {3}\n",
                col_display, topic, msg_count, data_type
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

fn print_size(
    file_path: PathBuf,
    metadata: &BagMetadata,
    use_types: bool,
    writer: &mut impl Write,
) -> Result<(), Error> {
    writer.write_all(
        format!(
            "{0: <13}{1}\n",
            "path:",
            metadata
                .file_path
                .as_ref()
                .map_or_else(|| "None".to_string(), |p| p.to_string_lossy().into_owned())
        )
        .as_bytes(),
    )?;
    writer
        .write_all(format!("{0: <13}{1}\n", "size:", human_bytes(metadata.num_bytes)).as_bytes())?;

    let topics_to_types: HashMap<&str, &str> = metadata.topics_and_types().into_iter().collect();
    let mut size_map = HashMap::new();
    let bag = DecompressedBag::from_file(file_path).unwrap();
    for msg_view in bag.read_messages(&Query::all()).unwrap() {
        let size = msg_view.size();
        if use_types {
            let data_type = *topics_to_types.get(msg_view.topic).unwrap();
            let entry: &mut u64 = size_map.entry(data_type).or_insert(0);
            *entry += size;
        } else {
            let entry = size_map.entry(msg_view.topic).or_insert(0);
            *entry += size;
        }
    }

    let start_padding = 4;

    let second_padding = {
        let key_len = if use_types {
            max_type_len(metadata)
        } else {
            max_topic_len(metadata)
        };
        key_len + 4
    };
    for (key, sum) in size_map
        .iter()
        .sorted_by_cached_key(|(_, sum)| -(**sum as i64))
    {
        writer.write_all(
            format!(
                "{0: <start_padding$}{1: <second_padding$} {2:>10}\n",
                "",
                key,
                human_bytes(*sum)
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

fn main() -> Result<(), Error> {
    let args = args();

    let stdout = std::io::stdout();
    let lock = stdout.lock();
    let mut writer = BufWriter::new(lock);

    match args {
        Opts::TopicOptions { file_path } => {
            let metadata = BagMetadata::from_file(file_path)?;
            print_topics(&metadata, &mut writer)
        }
        Opts::InfoOptions { minimal, file_path } => {
            let metadata = BagMetadata::from_file(file_path)?;
            print_all(&metadata, minimal, &mut writer)
        }
        Opts::TypeOptions { file_path } => {
            let metadata = BagMetadata::from_file(file_path)?;
            print_types(&metadata, &mut writer)
        }
        Opts::SizeOptions {
            use_types,
            file_path,
        } => {
            let metadata = BagMetadata::from_file(&file_path)?;
            print_size(file_path, &metadata, use_types, &mut writer)
        }
    }
}
