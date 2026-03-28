import os
import re
import zipfile
import logging
from dataclasses import dataclass, field
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)


@dataclass
class DataFileInfo:
    zip_path: str
    entry_path: str
    file_stem: str
    country_code: str
    recode_type: str
    dhs_phase: str
    file_format: str            # DTA | DAT
    dct_entry_path: Optional[str] = None


@dataclass
class ZipBundle:
    zip_path: str
    zip_name: str
    country_code: str
    year_label: str
    program_code: str
    data_files: List[DataFileInfo] = field(default_factory=list)


# ── filename parsers ─────────────────────────────────────────────────

def parse_zip_filename(fname: str) -> Tuple[str, str, str]:
    """Return (country_iso2, year_label, program_code) from a zip name."""
    # Standard DHS download: MW_2015-16_DHS_<download‑stamp>.zip
    m = re.match(
        r'^([A-Z]{2})_(\d{4}(?:-\d{2,4})?)_(DHS|MIS|AIS|SPA|KAP)_',
        fname, re.IGNORECASE,
    )
    if m:
        return m.group(1).upper(), m.group(2), m.group(3).upper()

    # Legacy single‑recode file: MWWI42FL.zip
    m = re.match(r'^([A-Z]{2})[A-Z]{2}(\d)\dFL\.zip$', fname, re.IGNORECASE)
    if m:
        return m.group(1).upper(), f"phase{m.group(2)}", "DHS"

    raise ValueError(f"Cannot parse zip filename: {fname}")


def parse_file_stem(stem: str) -> Tuple[str, str]:
    """Return (recode_type, dhs_phase) from a file stem like MWIR7AFL."""
    m = re.match(r'^[A-Z]{2}([A-Z]{2})(\d[A-Z])FL$', stem.upper())
    if m:
        return m.group(1), m.group(2)
    if len(stem) >= 4:
        return stem[2:4].upper(), ""
    return "UNK", ""


# ── zip introspection ────────────────────────────────────────────────

def _list_data_files(zip_path: str, country_code: str) -> List[DataFileInfo]:
    """Find loadable data files (.DTA preferred, .DAT as fallback)."""
    files: List[DataFileInfo] = []
    dta_stems: set = set()
    dct_map: dict = {}

    with zipfile.ZipFile(zip_path, 'r') as zf:
        entries = zf.namelist()

        for entry in entries:
            if entry.upper().endswith('.DCT'):
                stem = os.path.splitext(os.path.basename(entry))[0].upper()
                dct_map[stem] = entry

        for entry in entries:
            if entry.upper().endswith('.DTA'):
                stem = os.path.splitext(os.path.basename(entry))[0].upper()
                recode, phase = parse_file_stem(stem)
                files.append(DataFileInfo(
                    zip_path=zip_path, entry_path=entry, file_stem=stem,
                    country_code=country_code, recode_type=recode,
                    dhs_phase=phase, file_format="DTA",
                ))
                dta_stems.add(stem)

        # Only use .DAT when no .DTA exists for the same stem
        for entry in entries:
            if entry.upper().endswith('.DAT'):
                stem = os.path.splitext(os.path.basename(entry))[0].upper()
                if stem in dta_stems:
                    continue
                recode, phase = parse_file_stem(stem)
                files.append(DataFileInfo(
                    zip_path=zip_path, entry_path=entry, file_stem=stem,
                    country_code=country_code, recode_type=recode,
                    dhs_phase=phase, file_format="DAT",
                    dct_entry_path=dct_map.get(stem),
                ))

    return files


# ── public API ───────────────────────────────────────────────────────

def discover_zips(data_dir: str) -> List[ZipBundle]:
    """Walk *data_dir* and return one ZipBundle per usable archive."""
    bundles: List[ZipBundle] = []

    for fname in sorted(os.listdir(data_dir)):
        if not fname.lower().endswith('.zip'):
            continue
        if re.search(r'\(\d+\)\.zip$', fname):
            logger.info("Skipping duplicate: %s", fname)
            continue

        zip_path = os.path.join(data_dir, fname)
        try:
            country, year, program = parse_zip_filename(fname)
        except ValueError:
            logger.warning("Skipping unrecognised zip: %s", fname)
            continue

        data_files = _list_data_files(zip_path, country)
        if not data_files:
            logger.warning("No data files in %s", fname)
            continue

        bundles.append(ZipBundle(
            zip_path=zip_path, zip_name=fname,
            country_code=country, year_label=year,
            program_code=program, data_files=data_files,
        ))
        logger.info(
            "Discovered %s: %s %s %s (%d file(s))",
            fname, country, year, program, len(data_files),
        )

    return bundles
