import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.filesystem import CompressionTypes

import zipfile
import tarfile
import gzip
import bz2
import lzma

import os
import posixpath


class ExtractCompressedFilesToPath(beam.DoFn):
    """
    Extract-only DoFn:
      - Extract ZIP/TAR/TGZ/TAR.GZ into input_file_path
      - Decompress GZ / BZ2 / XZ into input_file_path
      - Writes outputs FLAT into input_file_path (no archive subfolder) if flat_output=True
      - Optionally prefixes output filenames with archive name to avoid collisions
      - After successful extraction, moves or deletes the source archive
      - Emits (src_path, out_path) for EACH extracted output file
    """

    def __init__(
        self,
        input_file_path,
        buffer_size=8 * 1024 * 1024,
        overwrite=True,
        flat_output=True,     # zip files will placed in input path without sub directory 
        prefix_archive_name=True,  
        processed_prefix=None,   # where to move archive (optional)
        delete_source=False      # if True and processed_prefix is None -> delete
    ):
        self.input_file_path = input_file_path.rstrip("/\\")
        self.buffer_size = int(buffer_size)
        self.overwrite = bool(overwrite)
        self.flat_output = bool(flat_output)
        self.prefix_archive_name = bool(prefix_archive_name)
        self.processed_prefix = processed_prefix.rstrip("/\\") if processed_prefix else None
        self.delete_source = bool(delete_source)

    # ---------------- path helpers ----------------
    def _join(self, base, *parts):
        if base.startswith("gs://"):
            p = base.rstrip("/")
            for x in parts:
                p = posixpath.join(p, x.replace("\\", "/"))
            return p
        return os.path.join(base, *parts)

    def _safe_rel(self, name: str) -> str:
        name = name.replace("\\", "/").lstrip("/")
        name = name.replace("..", "__")
        return name

    def _ensure_parent_dir_local(self, out_path: str):
        if not out_path.startswith("gs://"):
            os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # ---------------- write stream -> output file ----------------
    def _write_stream(self, fin, out_path: str):
        """
        fin: file-like object supporting .read()
        out_path: destination file path (local or gs://)
        """
        self._ensure_parent_dir_local(out_path)

        if self.overwrite:
            try:
                if FileSystems.exists(out_path):
                    FileSystems.delete([out_path])
            except Exception:
                pass

        with FileSystems.create(out_path) as fout:
            while True:
                chunk = fin.read(self.buffer_size)
                if not chunk:
                    break
                fout.write(chunk)

    def _strip_one_ext(self, filename: str, ext: str) -> str:
        return filename[:-len(ext)] if filename.lower().endswith(ext) else filename

    def _make_out_path(self, archive_base: str, inner_name: str) -> str:
        """
        Builds the final output path for an extracted/decompressed inner file.
        - flat_output=True => input_file_path/<file>
        - else            => input_file_path/<archive_base>/<inner_name>
        - prefix_archive_name=True => <archive_base>__<file> (collision-safe for flat)
        """

        inner_name = self._safe_rel(inner_name)
        
        # If truly flat output, keep only basename
        file_only = os.path.basename(inner_name)
        
        if self.prefix_archive_name and archive_base is not None:
            file_only = f"{archive_base}__{file_only}"

        if self.flat_output or archive_base is None :
            return self._join(self.input_file_path, file_only)
        else:
            return self._join(self.input_file_path, archive_base, inner_name)

    def _move_or_delete_source(self, src_path: str):
        """
        After extraction success:
        - If processed_prefix provided => move (copy+delete for gs:// and local)
        - Else if delete_source True   => delete
        - Else do nothing
        """
        if self.processed_prefix and not(self.delete_source):
            base = os.path.basename(src_path).replace("\\", "/")
            dest = self._join(self.processed_prefix, base)

            # ensure local directory exists
            if not dest.startswith("gs://"):
                os.makedirs(self.processed_prefix, exist_ok=True)

            FileSystems.copy([src_path], [dest])
            FileSystems.delete([src_path])
            return

        if self.delete_source:
            FileSystems.delete([src_path])

    # ---------------- main ----------------
    def process(self, readable_file: fileio.ReadableFile):
        src_path = readable_file.metadata.path
        name_lower = (src_path or "").lower()
        archive_base = os.path.basename(src_path).replace("\\", "/")
        # -------- ZIP (multi-file) --------
        if name_lower.endswith(".zip"):
            with readable_file.open() as fin:
                # NOTE: zipfile needs seekable streams. Works reliably for local files.
                with zipfile.ZipFile(fin) as zf:
                    for member in zf.infolist():
                        if member.is_dir():
                            continue
                        out_path = self._make_out_path(archive_base, member.filename)
                        with zf.open(member) as mf:
                            self._write_stream(mf, out_path)
                        yield (src_path, out_path)

            self._move_or_delete_source(src_path)
            return

        # -------- TAR/TGZ/TAR.GZ (multi-file streaming) --------
        if name_lower.endswith(".tar") or name_lower.endswith(".tar.gz") or name_lower.endswith(".tgz"):
            mode = "r|gz" if (name_lower.endswith(".tar.gz") or name_lower.endswith(".tgz")) else "r|"
            with readable_file.open() as fin:
                with tarfile.open(fileobj=fin, mode=mode) as tf:
                    for member in tf:
                        if not member.isfile():
                            continue
                        f = tf.extractfile(member)
                        if f is None:
                            continue
                        out_path = self._make_out_path(archive_base, member.name)
                        with f:
                            self._write_stream(f, out_path)
                        yield (src_path, out_path)

            self._move_or_delete_source(src_path)
            return

        # -------- GZ (single-file) --------
        # IMPORTANT: open RAW bytes (UNCOMPRESSED), do not rely on readable_file.open() auto wrapping
        if name_lower.endswith(".gz") and not name_lower.endswith(".tar.gz") and not name_lower.endswith(".tgz"):
            inner_name = self._strip_one_ext(archive_base, ".gz")
            out_path = self._make_out_path(None, inner_name)
            with FileSystems.open(src_path, compression_type=CompressionTypes.UNCOMPRESSED) as fin:
                with gzip.GzipFile(fileobj=fin) as gz:
                    self._write_stream(gz, out_path)

            self._move_or_delete_source(src_path)
            yield (src_path, out_path)
            return

        # -------- BZ2 (single-file) --------
        if name_lower.endswith(".bz2"):
            inner_name = self._strip_one_ext(archive_base, ".bz2")
            out_path = self._make_out_path(None, inner_name)

            with FileSystems.open(src_path, compression_type=CompressionTypes.UNCOMPRESSED) as fin:
                with bz2.BZ2File(fin) as bz:
                    self._write_stream(bz, out_path)

            self._move_or_delete_source(src_path)
            yield (src_path, out_path)
            return

        # -------- XZ (single-file) --------
        if name_lower.endswith(".xz"):
            inner_name = self._strip_one_ext(archive_base, ".xz")
            out_path = self._make_out_path(None, inner_name)

            with FileSystems.open(src_path, compression_type=CompressionTypes.UNCOMPRESSED) as fin:
                with lzma.LZMAFile(fin) as xz:
                    self._write_stream(xz, out_path)

            self._move_or_delete_source(src_path)
            yield (src_path, out_path)
            return

        # Non-supported => do nothing
        return