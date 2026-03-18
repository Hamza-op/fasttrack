use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::app::types::CameraMetadata;

#[allow(dead_code)]
pub fn detect_camera_metadata(path: &Path) -> CameraMetadata {
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();

    if !matches!(
        ext.as_str(),
        "arw"
            | "sr2"
            | "srf"
            | "cr2"
            | "cr3"
            | "crw"
            | "nef"
            | "nrw"
            | "raf"
            | "dng"
            | "rw2"
            | "orf"
            | "pef"
            | "srw"
            | "iiq"
            | "jpg"
            | "jpeg"
            | "heic"
            | "heif"
            | "hif"
            | "tif"
            | "tiff"
    ) {
        return CameraMetadata::default();
    }

    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return CameraMetadata::default(),
    };
    let mut reader = BufReader::new(file);
    let exif = match exif::Reader::new().read_from_container(&mut reader) {
        Ok(exif) => exif,
        Err(_) => return CameraMetadata::default(),
    };

    let make = exif
        .get_field(exif::Tag::Make, exif::In::PRIMARY)
        .map(|field| field.display_value().with_unit(&exif).to_string())
        .filter(|value| !value.is_empty());

    let model = exif
        .get_field(exif::Tag::Model, exif::In::PRIMARY)
        .map(|field| field.display_value().with_unit(&exif).to_string())
        .filter(|value| !value.is_empty());

    let serial_number = exif
        .get_field(exif::Tag::BodySerialNumber, exif::In::PRIMARY)
        .map(|field| field.display_value().with_unit(&exif).to_string())
        .filter(|value| !value.is_empty());

    CameraMetadata {
        make,
        model,
        serial_number,
    }
}
