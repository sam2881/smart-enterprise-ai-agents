"""
Legacy Source Utilities - DATA PLANE

Utilities for legacy/enterprise sources:
- DTSX/SSIS: SQL Server Integration Services package parsing
- EBCDIC: Mainframe EBCDIC data decoding with COBOL copybooks
- Fixed-Width: Fixed-width file parsing with layout definitions
- COBOL Copybook: Copybook parsing for field layout extraction
- AS400/Mainframe: IBM AS/400 and mainframe extract handling

Used by zone_processor.py for source reading in the Data Plane.
All behavior driven by metadata configuration.
"""

from typing import Any, Dict, List, Optional, Tuple
import logging
import re

logger = logging.getLogger(__name__)


# =============================================================================
# COBOL PIC Clause Type Mapping
# =============================================================================

PIC_TYPE_MAP = {
    "X": "string",      # Alphanumeric
    "A": "string",      # Alphabetic
    "9": "integer",     # Numeric (no decimal)
    "S9": "integer",    # Signed numeric
    "V9": "decimal",    # Numeric with implied decimal
    "S9V9": "decimal",  # Signed numeric with implied decimal
}

# EBCDIC encoding map
EBCDIC_ENCODINGS = {
    "us": "cp037",
    "international": "cp500",
    "uk": "cp285",
    "german": "cp273",
    "french": "cp297",
    "japanese": "cp930",
    "chinese": "cp935",
}


class DTSXParser:
    """
    DTSX (SSIS Package) Parser.

    Parses SQL Server Integration Services packages and extracts:
    - Data flow components (sources, transforms, destinations)
    - Connection managers (OLEDB, ADO.NET, Flat File)
    - Package variables and parameters
    - Precedence constraints (execution order)

    Extraction is used to generate equivalent Airflow DAG + Spark jobs.
    """

    def __init__(self):
        self.package = None
        self._namespace = {
            "DTS": "www.microsoft.com/SqlServer/Dts",
            "SQLTask": "www.microsoft.com/sqlserver/dts/tasks/sqltask",
        }

    def validate_package(self, dtsx_path: str) -> bool:
        """
        Validate DTSX package exists and is parseable.

        Args:
            dtsx_path: Path to DTSX file (GCS or local)

        Returns:
            True if valid, False otherwise
        """
        try:
            content = self._read_file(dtsx_path)
            if not content:
                return False

            import xml.etree.ElementTree as ET
            root = ET.fromstring(content)

            # Check for DTS:Executable root element
            return root.tag.endswith("Executable") or "DTS" in root.tag

        except Exception as e:
            logger.warning(f"DTSX validation failed: {e}")
            return False

    def parse(self, dtsx_path: str) -> Dict[str, Any]:
        """
        Parse DTSX package and extract components.

        Args:
            dtsx_path: Path to DTSX file

        Returns:
            Parsed package structure with data_flows, connections, variables
        """
        content = self._read_file(dtsx_path)
        if not content:
            return self._empty_package(dtsx_path)

        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(content)

            package_name = root.attrib.get(
                "{www.microsoft.com/SqlServer/Dts}ObjectName",
                root.attrib.get("DTS:ObjectName", "unknown")
            )

            return {
                "package_name": package_name,
                "data_flows": self._extract_data_flows(root),
                "connection_managers": self._extract_connections(root),
                "variables": self._extract_variables(root),
                "precedence_constraints": self._extract_precedence(root),
                "source_file": dtsx_path,
            }

        except Exception as e:
            logger.error(f"DTSX parse failed: {e}")
            return self._empty_package(dtsx_path)

    def extract_sources(self, parsed: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract source components from parsed package."""
        sources = []
        for flow in parsed.get("data_flows", []):
            for component in flow.get("components", []):
                if component.get("type") in ("OLE DB Source", "Flat File Source", "ADO NET Source"):
                    sources.append({
                        "name": component.get("name"),
                        "type": component.get("type"),
                        "connection": component.get("connection"),
                        "sql_command": component.get("sql_command"),
                    })
        return sources

    def extract_destinations(self, parsed: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract destination components from parsed package."""
        destinations = []
        for flow in parsed.get("data_flows", []):
            for component in flow.get("components", []):
                if component.get("type") in ("OLE DB Destination", "Flat File Destination", "ADO NET Destination"):
                    destinations.append({
                        "name": component.get("name"),
                        "type": component.get("type"),
                        "connection": component.get("connection"),
                        "table": component.get("table"),
                    })
        return destinations

    def map_to_apex(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map parsed DTSX structure to APEX pipeline config.

        Returns:
            APEX-compatible pipeline configuration
        """
        sources = self.extract_sources(parsed)
        destinations = self.extract_destinations(parsed)

        # Determine source type from DTSX components
        source_type = "file_csv"  # default
        if sources:
            src_type = sources[0].get("type", "")
            if "OLE DB" in src_type or "ADO NET" in src_type:
                source_type = "database_sqlserver"
            elif "Flat File" in src_type:
                source_type = "file_csv"

        return {
            "feed": {
                "feed_name": parsed.get("package_name", "migrated_feed"),
                "description": f"Migrated from SSIS: {parsed.get('source_file', '')}",
            },
            "source": {
                "source_type": source_type,
                "original_sources": sources,
            },
            "target": {
                "original_destinations": destinations,
            },
            "migration_metadata": {
                "original_package": parsed.get("package_name"),
                "original_path": parsed.get("source_file"),
                "components_count": sum(
                    len(f.get("components", [])) for f in parsed.get("data_flows", [])
                ),
                "connections_count": len(parsed.get("connection_managers", [])),
            },
        }

    def _extract_data_flows(self, root) -> List[Dict[str, Any]]:
        """Extract data flow tasks from XML."""
        flows = []
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "Executable":
                obj_name = elem.attrib.get(
                    "{www.microsoft.com/SqlServer/Dts}ObjectName",
                    elem.attrib.get("DTS:ObjectName", "")
                )
                if obj_name:
                    flows.append({
                        "name": obj_name,
                        "components": self._extract_components(elem),
                    })
        return flows

    def _extract_components(self, flow_elem) -> List[Dict[str, Any]]:
        """Extract pipeline components from a data flow."""
        components = []
        for elem in flow_elem.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag in ("component", "PipelineComponent"):
                name = elem.attrib.get("name", elem.attrib.get("componentClassID", "unknown"))
                comp_type = elem.attrib.get("componentClassID", "")
                components.append({
                    "name": name,
                    "type": self._classify_component(comp_type),
                    "class_id": comp_type,
                })
        return components

    def _classify_component(self, class_id: str) -> str:
        """Classify SSIS component by class ID."""
        class_id_lower = class_id.lower()
        if "source" in class_id_lower:
            return "OLE DB Source"
        if "destination" in class_id_lower:
            return "OLE DB Destination"
        if "transform" in class_id_lower or "convert" in class_id_lower:
            return "Transform"
        return "Unknown"

    def _extract_connections(self, root) -> List[Dict[str, Any]]:
        """Extract connection managers."""
        connections = []
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "ConnectionManager":
                name = elem.attrib.get(
                    "{www.microsoft.com/SqlServer/Dts}ObjectName",
                    elem.attrib.get("DTS:ObjectName", "unknown")
                )
                connections.append({
                    "name": name,
                    "creation_name": elem.attrib.get(
                        "{www.microsoft.com/SqlServer/Dts}CreationName",
                        elem.attrib.get("DTS:CreationName", "")
                    ),
                })
        return connections

    def _extract_variables(self, root) -> Dict[str, Any]:
        """Extract package variables."""
        variables = {}
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "Variable":
                name = elem.attrib.get(
                    "{www.microsoft.com/SqlServer/Dts}ObjectName",
                    elem.attrib.get("DTS:ObjectName", "")
                )
                if name:
                    variables[name] = elem.attrib.get("DTS:Expression", "")
        return variables

    def _extract_precedence(self, root) -> List[Dict[str, Any]]:
        """Extract precedence constraints (execution order)."""
        constraints = []
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "PrecedenceConstraint":
                constraints.append({
                    "from": elem.attrib.get("DTS:From", ""),
                    "to": elem.attrib.get("DTS:To", ""),
                    "value": elem.attrib.get("DTS:Value", "Success"),
                })
        return constraints

    def _read_file(self, path: str) -> Optional[str]:
        """Read file from GCS or local filesystem."""
        if path.startswith("gs://"):
            try:
                from google.cloud import storage
                parts = path[5:].split("/", 1)
                bucket_name, blob_path = parts[0], parts[1]
                client = storage.Client()
                bucket = client.bucket(bucket_name)
                blob = bucket.blob(blob_path)
                return blob.download_as_text()
            except (ImportError, Exception) as e:
                logger.warning(f"Cannot read GCS file {path}: {e}")
                return None
        else:
            try:
                with open(path, "r") as f:
                    return f.read()
            except FileNotFoundError:
                return None

    def _empty_package(self, path: str) -> Dict[str, Any]:
        """Return empty package structure."""
        return {
            "package_name": "unknown",
            "data_flows": [],
            "connection_managers": [],
            "variables": {},
            "precedence_constraints": [],
            "source_file": path,
        }


class EBCDICDecoder:
    """
    EBCDIC Data Decoder.

    Decodes EBCDIC-encoded mainframe data using COBOL copybooks.
    Supports:
    - Standard EBCDIC encodings (cp037 US, cp500 International, etc.)
    - Variable-length records (with RDW - Record Descriptor Word)
    - Packed decimal (COMP-3) fields
    - Binary (COMP) fields
    - Zoned decimal fields
    """

    def __init__(self, copybook_path: Optional[str] = None):
        """
        Initialize decoder.

        Args:
            copybook_path: Path to COBOL copybook for field layout
        """
        self.copybook_path = copybook_path
        self.layout: Optional[Dict[str, Any]] = None
        if copybook_path:
            self.layout = self.parse_copybook(copybook_path)

    def parse_copybook(self, copybook_path: str) -> Dict[str, Any]:
        """
        Parse COBOL copybook to extract field layout.

        Args:
            copybook_path: Path to copybook file

        Returns:
            Field layout with positions, lengths, and types
        """
        content = self._read_copybook(copybook_path)
        if not content:
            return {"record_length": 0, "fields": []}

        fields = []
        current_offset = 0

        for line in content.split("\n"):
            line = line.strip()
            if not line or line.startswith("*"):
                continue

            field = self._parse_pic_clause(line, current_offset)
            if field:
                fields.append(field)
                current_offset += field["length"]

        record_length = sum(f["length"] for f in fields)

        return {
            "record_length": record_length,
            "fields": fields,
        }

    def decode_record(
        self,
        record: bytes,
        encoding: str = "cp037",
    ) -> Dict[str, Any]:
        """
        Decode a single EBCDIC record using the loaded layout.

        Args:
            record: Raw EBCDIC bytes
            encoding: EBCDIC encoding (cp037 for US, cp500 for international)

        Returns:
            Dictionary of decoded field values
        """
        if not self.layout or not self.layout.get("fields"):
            return {}

        result = {}
        for field in self.layout["fields"]:
            start = field["offset"]
            length = field["length"]
            field_bytes = record[start:start + length]

            if field["pic_type"] == "COMP-3":
                result[field["name"]] = self._decode_packed_decimal(field_bytes, field.get("scale", 0))
            elif field["pic_type"] == "COMP":
                result[field["name"]] = int.from_bytes(field_bytes, byteorder="big", signed=True)
            else:
                result[field["name"]] = field_bytes.decode(encoding, errors="replace").strip()

        return result

    def decode_file(
        self,
        file_path: str,
        encoding: str = "cp037",
        max_records: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Decode an entire EBCDIC file.

        Args:
            file_path: Path to EBCDIC data file
            encoding: EBCDIC encoding
            max_records: Max records to decode (None for all)

        Returns:
            List of decoded records
        """
        if not self.layout:
            return []

        record_length = self.layout["record_length"]
        records = []

        try:
            data = self._read_binary(file_path)
            if not data:
                return []

            offset = 0
            while offset + record_length <= len(data):
                record = data[offset:offset + record_length]
                decoded = self.decode_record(record, encoding)
                records.append(decoded)
                offset += record_length

                if max_records and len(records) >= max_records:
                    break

        except Exception as e:
            logger.error(f"EBCDIC decode failed: {e}")

        return records

    def get_spark_schema(self) -> List[Dict[str, str]]:
        """
        Convert COBOL layout to Spark-compatible schema.

        Returns:
            List of {name, type} dicts for Spark StructType
        """
        if not self.layout:
            return []

        spark_types = []
        for field in self.layout["fields"]:
            spark_type = "string"  # default
            if field.get("data_type") == "integer":
                spark_type = "long"
            elif field.get("data_type") == "decimal":
                scale = field.get("scale", 0)
                precision = field.get("length", 10)
                spark_type = f"decimal({precision},{scale})"

            spark_types.append({
                "name": field["name"].lower().replace("-", "_"),
                "type": spark_type,
            })

        return spark_types

    def _parse_pic_clause(self, line: str, offset: int) -> Optional[Dict[str, Any]]:
        """Parse a single PIC clause line."""
        # Match patterns like: 05 CUSTOMER-ID PIC X(10).
        match = re.match(
            r"\s*(\d+)\s+([\w-]+)\s+PIC\s+([SXA9V().\-]+)",
            line, re.IGNORECASE
        )
        if not match:
            return None

        level = int(match.group(1))
        name = match.group(2)
        pic = match.group(3).rstrip(".")

        length, data_type, pic_type, scale = self._interpret_pic(pic)

        return {
            "level": level,
            "name": name,
            "pic": pic,
            "pic_type": pic_type,
            "data_type": data_type,
            "length": length,
            "offset": offset,
            "scale": scale,
        }

    def _interpret_pic(self, pic: str) -> Tuple[int, str, str, int]:
        """Interpret PIC clause to determine length, type, and scale."""
        pic_upper = pic.upper().replace(" ", "")

        # X(n) - alphanumeric
        match = re.match(r"X\((\d+)\)", pic_upper)
        if match:
            return int(match.group(1)), "string", "DISPLAY", 0

        # X repeated
        if re.match(r"X+$", pic_upper):
            return len(pic_upper), "string", "DISPLAY", 0

        # 9(n)V9(m) or S9(n)V9(m) - decimal
        match = re.match(r"S?9\((\d+)\)V9\((\d+)\)", pic_upper)
        if match:
            int_len = int(match.group(1))
            dec_len = int(match.group(2))
            return int_len + dec_len, "decimal", "DISPLAY", dec_len

        # 9(n) - integer
        match = re.match(r"S?9\((\d+)\)", pic_upper)
        if match:
            return int(match.group(1)), "integer", "DISPLAY", 0

        # 9 repeated
        if re.match(r"S?9+$", pic_upper):
            count = len(pic_upper.replace("S", ""))
            return count, "integer", "DISPLAY", 0

        # Default: treat as string of PIC length
        return max(len(pic_upper), 1), "string", "DISPLAY", 0

    def _decode_packed_decimal(self, data: bytes, scale: int = 0) -> float:
        """Decode COMP-3 packed decimal."""
        digits = []
        for byte in data[:-1]:
            digits.append(str((byte >> 4) & 0x0F))
            digits.append(str(byte & 0x0F))
        # Last byte: high nibble is digit, low nibble is sign
        last = data[-1]
        digits.append(str((last >> 4) & 0x0F))
        sign = last & 0x0F

        value = int("".join(digits))
        if sign in (0x0B, 0x0D):  # Negative signs
            value = -value

        if scale > 0:
            return value / (10 ** scale)
        return float(value)

    def _read_copybook(self, path: str) -> Optional[str]:
        """Read copybook from GCS or local."""
        if path.startswith("gs://"):
            try:
                from google.cloud import storage
                parts = path[5:].split("/", 1)
                client = storage.Client()
                blob = client.bucket(parts[0]).blob(parts[1])
                return blob.download_as_text()
            except (ImportError, Exception):
                return None
        try:
            with open(path, "r") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def _read_binary(self, path: str) -> Optional[bytes]:
        """Read binary file from GCS or local."""
        if path.startswith("gs://"):
            try:
                from google.cloud import storage
                parts = path[5:].split("/", 1)
                client = storage.Client()
                blob = client.bucket(parts[0]).blob(parts[1])
                return blob.download_as_bytes()
            except (ImportError, Exception):
                return None
        try:
            with open(path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None


class FixedWidthParser:
    """
    Fixed-Width File Parser.

    Parses fixed-width format files using layout definitions.
    Layout defines field name, start position, length, and type.
    """

    def __init__(self, layout: Optional[List[Dict[str, Any]]] = None):
        """
        Initialize parser.

        Args:
            layout: List of field definitions [{name, start, length, type}]
        """
        self.layout = layout or []

    @staticmethod
    def validate_layout(layout_file: str) -> bool:
        """
        Validate layout file format.

        Args:
            layout_file: Path to layout definition file (JSON)

        Returns:
            True if valid layout
        """
        try:
            import json
            with open(layout_file) as f:
                data = json.load(f)
            return isinstance(data, list) and all(
                "name" in f and "start" in f and "length" in f
                for f in data
            )
        except Exception:
            return False

    @staticmethod
    def parse_layout(layout_file: str) -> List[Dict[str, Any]]:
        """
        Parse layout definition from JSON file.

        Args:
            layout_file: Path to layout JSON file

        Returns:
            List of field definitions
        """
        try:
            import json
            with open(layout_file) as f:
                return json.load(f)
        except Exception:
            return []

    def parse_line(self, line: str) -> Dict[str, str]:
        """
        Parse a single line according to layout.

        Args:
            line: Raw fixed-width line

        Returns:
            Dictionary of field values
        """
        result = {}
        for field in self.layout:
            start = field["start"]
            length = field["length"]
            value = line[start:start + length].strip()
            result[field["name"]] = value
        return result

    def parse_file(
        self,
        file_path: str,
        skip_header: int = 0,
        skip_footer: int = 0,
        max_records: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        """
        Parse entire fixed-width file.

        Args:
            file_path: Path to fixed-width file
            skip_header: Lines to skip at start
            skip_footer: Lines to skip at end
            max_records: Maximum records to parse

        Returns:
            List of parsed records
        """
        records = []
        try:
            with open(file_path, "r") as f:
                lines = f.readlines()

            # Strip header/footer
            if skip_footer > 0:
                lines = lines[skip_header:-skip_footer]
            else:
                lines = lines[skip_header:]

            for line in lines:
                if max_records and len(records) >= max_records:
                    break
                records.append(self.parse_line(line))

        except Exception as e:
            logger.error(f"Fixed-width parse failed: {e}")

        return records

    def get_spark_schema(self) -> List[Dict[str, str]]:
        """Convert layout to Spark schema."""
        return [
            {"name": f["name"], "type": f.get("type", "string")}
            for f in self.layout
        ]


class COBOLCopybookParser:
    """
    COBOL Copybook Parser.

    Standalone parser for COBOL copybook definitions.
    Used for EBCDIC data layout extraction without requiring
    the full EBCDICDecoder.
    """

    @staticmethod
    def validate(copybook_path: str) -> bool:
        """
        Validate COBOL copybook syntax.

        Args:
            copybook_path: Path to copybook file

        Returns:
            True if valid COBOL copybook
        """
        try:
            with open(copybook_path, "r") as f:
                content = f.read()
            # Check for PIC clauses
            return bool(re.search(r"PIC\s+[XA9SV()\-]+", content, re.IGNORECASE))
        except Exception:
            return False

    @staticmethod
    def parse(copybook_path: str) -> Dict[str, Any]:
        """
        Parse COBOL copybook into structured layout.

        Args:
            copybook_path: Path to copybook file

        Returns:
            Parsed layout with fields, record_length, pic_clauses
        """
        decoder = EBCDICDecoder(copybook_path)
        layout = decoder.layout or {"record_length": 0, "fields": []}

        pic_clauses = {}
        for field in layout.get("fields", []):
            pic_clauses[field["name"]] = field.get("pic", "")

        return {
            "fields": layout.get("fields", []),
            "record_length": layout.get("record_length", 0),
            "pic_clauses": pic_clauses,
        }

    @staticmethod
    def to_spark_schema(copybook_path: str) -> List[Dict[str, str]]:
        """
        Convert copybook to Spark-compatible schema.

        Args:
            copybook_path: Path to copybook file

        Returns:
            List of {name, type} for StructType
        """
        decoder = EBCDICDecoder(copybook_path)
        return decoder.get_spark_schema()


class MainframeExtractHandler:
    """
    Mainframe Extract Handler.

    Handles common mainframe extract patterns:
    - Sequential files with EBCDIC encoding
    - Variable-length records (RDW headers)
    - Multi-record types in single file
    - Trailer records with record counts
    """

    def __init__(self, encoding: str = "cp037"):
        """
        Initialize handler.

        Args:
            encoding: EBCDIC encoding (default cp037 for US)
        """
        self.encoding = encoding

    def detect_record_type(
        self,
        record: bytes,
        type_field_offset: int = 0,
        type_field_length: int = 2,
    ) -> str:
        """
        Detect record type from a type indicator field.

        Args:
            record: Raw record bytes
            type_field_offset: Offset of type indicator
            type_field_length: Length of type indicator

        Returns:
            Record type string
        """
        type_bytes = record[type_field_offset:type_field_offset + type_field_length]
        return type_bytes.decode(self.encoding, errors="replace").strip()

    def read_variable_length_records(
        self,
        file_path: str,
        rdw_length: int = 4,
    ) -> List[bytes]:
        """
        Read variable-length records with RDW headers.

        Args:
            file_path: Path to data file
            rdw_length: RDW header length (typically 4 bytes)

        Returns:
            List of raw record bytes (without RDW)
        """
        records = []
        try:
            with open(file_path, "rb") as f:
                data = f.read()

            offset = 0
            while offset < len(data):
                if offset + rdw_length > len(data):
                    break

                # RDW: first 2 bytes = record length (big-endian)
                rec_len = int.from_bytes(data[offset:offset + 2], "big")
                if rec_len < rdw_length:
                    break

                record = data[offset + rdw_length:offset + rec_len]
                records.append(record)
                offset += rec_len

        except Exception as e:
            logger.error(f"Variable-length read failed: {e}")

        return records

    def validate_trailer(
        self,
        records: List[bytes],
        expected_count_offset: int = 0,
        expected_count_length: int = 10,
    ) -> Dict[str, Any]:
        """
        Validate trailer record against record count.

        Args:
            records: All records including trailer
            expected_count_offset: Offset of count field in trailer
            expected_count_length: Length of count field

        Returns:
            Validation result with actual/expected counts
        """
        if not records:
            return {"valid": False, "reason": "No records"}

        trailer = records[-1]
        data_records = records[:-1]

        try:
            count_str = trailer[expected_count_offset:expected_count_offset + expected_count_length]
            expected = int(count_str.decode(self.encoding, errors="replace").strip())
            actual = len(data_records)

            return {
                "valid": actual == expected,
                "expected_count": expected,
                "actual_count": actual,
                "difference": actual - expected,
            }
        except (ValueError, IndexError):
            return {
                "valid": False,
                "reason": "Could not parse trailer count",
                "actual_count": len(data_records),
            }


__all__ = [
    "DTSXParser",
    "EBCDICDecoder",
    "FixedWidthParser",
    "COBOLCopybookParser",
    "MainframeExtractHandler",
    "EBCDIC_ENCODINGS",
    "PIC_TYPE_MAP",
]
