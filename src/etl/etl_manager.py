"""
Healthcare Data Lakehouse ETL Manager.

Production-grade ETL orchestration for regulated healthcare data:
- Zone-based processing (Bronze → Silver → Gold → Platinum)
- Data quality gates with automatic quarantine
- Full lineage tracking for audit compliance
- Incremental and full load patterns
- Schema evolution with backward compatibility
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable

import structlog

from healthcare_data_lakehouse.src.lineage.lineage_tracker import (
    LineageTracker,
    LineageNode,
    TransformationType,
)
from healthcare_data_lakehouse.src.quality.data_quality import (
    DataQualityValidator,
    DataZone,
    QualityReport,
    QualityStatus,
)

logger = structlog.get_logger()


class LoadType(Enum):
    """ETL load patterns."""
    
    FULL = "full"                # Complete data refresh
    INCREMENTAL = "incremental"  # Delta changes only
    MERGE = "merge"              # Upsert pattern
    APPEND = "append"            # Insert only


class ETLStatus(Enum):
    """ETL job execution status."""
    
    PENDING = "pending"
    RUNNING = "running"
    QUALITY_CHECK = "quality_check"
    PROMOTING = "promoting"
    COMPLETED = "completed"
    FAILED = "failed"
    QUARANTINED = "quarantined"


@dataclass
class ETLJobConfig:
    """ETL job configuration."""
    
    job_id: str
    source_name: str
    target_zone: DataZone
    load_type: LoadType
    required_fields: list[str] = field(default_factory=list)
    partition_columns: list[str] = field(default_factory=list)
    dedup_columns: list[str] = field(default_factory=list)
    transformations: list[str] = field(default_factory=list)
    quality_threshold: float = 0.95
    enable_lineage: bool = True
    enable_quarantine: bool = True


@dataclass
class ETLJobResult:
    """ETL job execution result."""
    
    job_id: str
    run_id: str
    status: ETLStatus
    source_zone: DataZone
    target_zone: DataZone
    records_read: int
    records_written: int
    records_quarantined: int
    quality_report: QualityReport | None
    lineage_node_id: str | None
    start_time: datetime
    end_time: datetime | None
    error_message: str | None = None
    
    @property
    def duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "run_id": self.run_id,
            "status": self.status.value,
            "source_zone": self.source_zone.value,
            "target_zone": self.target_zone.value,
            "records_read": self.records_read,
            "records_written": self.records_written,
            "records_quarantined": self.records_quarantined,
            "quality_score": self.quality_report.overall_score if self.quality_report else None,
            "lineage_node_id": self.lineage_node_id,
            "duration_seconds": self.duration_seconds,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "error_message": self.error_message,
        }


@dataclass
class TransformSpec:
    """Transformation specification."""
    
    name: str
    transform_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    description: str = ""


class HealthcareETLManager:
    """
    Production ETL manager for healthcare data lakehouse.
    
    Features:
    - Zone-based medallion architecture
    - Built-in quality gates
    - Automatic lineage tracking
    - Quarantine for failed records
    - Transformation registry
    """
    
    # Zone progression order
    ZONE_ORDER = [
        DataZone.RAW,
        DataZone.BRONZE,
        DataZone.SILVER,
        DataZone.GOLD,
        DataZone.PLATINUM,
    ]
    
    def __init__(
        self,
        lineage_tracker: LineageTracker | None = None,
    ):
        self.lineage_tracker = lineage_tracker or LineageTracker()
        self.transformations: dict[str, TransformSpec] = {}
        self._init_standard_transforms()
        
        # In-memory storage for demo (replace with actual storage)
        self.zones: dict[DataZone, dict[str, list[dict[str, Any]]]] = {
            zone: {} for zone in DataZone
        }
        self.quarantine: dict[str, list[dict[str, Any]]] = {}
    
    def _init_standard_transforms(self) -> None:
        """Register standard healthcare transformations."""
        
        self.register_transform(TransformSpec(
            name="deduplicate",
            transform_fn=self._transform_deduplicate,
            description="Remove duplicate records by ID",
        ))
        
        self.register_transform(TransformSpec(
            name="standardize_dates",
            transform_fn=self._transform_standardize_dates,
            description="Convert dates to ISO 8601 format",
        ))
        
        self.register_transform(TransformSpec(
            name="uppercase_codes",
            transform_fn=self._transform_uppercase_codes,
            description="Standardize medical codes to uppercase",
        ))
        
        self.register_transform(TransformSpec(
            name="trim_strings",
            transform_fn=self._transform_trim_strings,
            description="Trim whitespace from string fields",
        ))
        
        self.register_transform(TransformSpec(
            name="null_handling",
            transform_fn=self._transform_null_handling,
            description="Standardize null/empty value representation",
        ))
        
        self.register_transform(TransformSpec(
            name="add_metadata",
            transform_fn=self._transform_add_metadata,
            description="Add processing metadata fields",
        ))
    
    def register_transform(self, spec: TransformSpec) -> None:
        """Register a transformation."""
        self.transformations[spec.name] = spec
        logger.info("transform_registered", name=spec.name)
    
    def run_job(
        self,
        config: ETLJobConfig,
        source_records: list[dict[str, Any]],
    ) -> ETLJobResult:
        """
        Execute ETL job with quality gates and lineage tracking.
        
        Args:
            config: Job configuration
            source_records: Input data records
            
        Returns:
            Job execution result
        """
        run_id = self._generate_run_id(config.job_id)
        start_time = datetime.utcnow()
        
        logger.info(
            "etl_job_started",
            job_id=config.job_id,
            run_id=run_id,
            source=config.source_name,
            target_zone=config.target_zone.value,
            record_count=len(source_records),
        )
        
        # Determine source zone
        source_zone = self._get_source_zone(config.target_zone)
        
        result = ETLJobResult(
            job_id=config.job_id,
            run_id=run_id,
            status=ETLStatus.RUNNING,
            source_zone=source_zone,
            target_zone=config.target_zone,
            records_read=len(source_records),
            records_written=0,
            records_quarantined=0,
            quality_report=None,
            lineage_node_id=None,
            start_time=start_time,
            end_time=None,
        )
        
        try:
            # Apply transformations
            transformed = source_records
            for transform_name in config.transformations:
                if transform_name in self.transformations:
                    spec = self.transformations[transform_name]
                    transformed = spec.transform_fn(transformed)
                    logger.debug(
                        "transform_applied",
                        transform=transform_name,
                        record_count=len(transformed),
                    )
            
            # Quality validation
            result.status = ETLStatus.QUALITY_CHECK
            validator = DataQualityValidator(
                dataset_name=config.source_name,
                id_field="id",
            )
            
            quality_report = validator.validate(
                records=transformed,
                target_zone=config.target_zone,
                required_fields=config.required_fields,
            )
            result.quality_report = quality_report
            
            # Check quality gate
            if not quality_report.promotion_eligible:
                if config.enable_quarantine:
                    # Quarantine failed records
                    quarantine_ids = set(quality_report.quarantine_records)
                    quarantined = [
                        r for r in transformed
                        if str(r.get("id", "")) in quarantine_ids
                    ]
                    passed = [
                        r for r in transformed
                        if str(r.get("id", "")) not in quarantine_ids
                    ]
                    
                    self._quarantine_records(
                        config.job_id,
                        quarantined,
                        quality_report,
                    )
                    result.records_quarantined = len(quarantined)
                    transformed = passed
                else:
                    # Fail entire job
                    result.status = ETLStatus.FAILED
                    result.end_time = datetime.utcnow()
                    result.error_message = (
                        f"Quality gate failed: score={quality_report.overall_score:.2f}"
                    )
                    return result
            
            # Promote to target zone
            result.status = ETLStatus.PROMOTING
            
            # Track lineage
            lineage_node_id = None
            if config.enable_lineage:
                lineage_node_id = self._track_lineage(
                    config,
                    source_records,
                    transformed,
                    quality_report,
                )
                result.lineage_node_id = lineage_node_id
            
            # Write to target zone
            self._write_to_zone(
                config.target_zone,
                config.source_name,
                transformed,
                config.load_type,
            )
            
            result.records_written = len(transformed)
            result.status = ETLStatus.COMPLETED
            result.end_time = datetime.utcnow()
            
            logger.info(
                "etl_job_completed",
                job_id=config.job_id,
                run_id=run_id,
                records_written=result.records_written,
                records_quarantined=result.records_quarantined,
                quality_score=quality_report.overall_score,
                duration_seconds=result.duration_seconds,
            )
            
        except Exception as e:
            result.status = ETLStatus.FAILED
            result.end_time = datetime.utcnow()
            result.error_message = str(e)
            
            logger.error(
                "etl_job_failed",
                job_id=config.job_id,
                run_id=run_id,
                error=str(e),
            )
        
        return result
    
    def _generate_run_id(self, job_id: str) -> str:
        """Generate unique run ID."""
        timestamp = datetime.utcnow().isoformat()
        content = f"{job_id}:{timestamp}"
        return f"run_{hashlib.sha256(content.encode()).hexdigest()[:12]}"
    
    def _get_source_zone(self, target_zone: DataZone) -> DataZone:
        """Get source zone for target zone promotion."""
        idx = self.ZONE_ORDER.index(target_zone)
        if idx > 0:
            return self.ZONE_ORDER[idx - 1]
        return DataZone.RAW
    
    def _quarantine_records(
        self,
        job_id: str,
        records: list[dict[str, Any]],
        quality_report: QualityReport,
    ) -> None:
        """Move failed records to quarantine."""
        if job_id not in self.quarantine:
            self.quarantine[job_id] = []
        
        for record in records:
            record["_quarantine_time"] = datetime.utcnow().isoformat()
            record["_quarantine_reason"] = quality_report.overall_status.value
            record["_quality_score"] = quality_report.overall_score
        
        self.quarantine[job_id].extend(records)
        
        logger.warning(
            "records_quarantined",
            job_id=job_id,
            count=len(records),
            reason=quality_report.overall_status.value,
        )
    
    def _track_lineage(
        self,
        config: ETLJobConfig,
        source_records: list[dict[str, Any]],
        output_records: list[dict[str, Any]],
        quality_report: QualityReport,
    ) -> str:
        """Track data lineage for transformation."""
        # Create source node
        source_node = self.lineage_tracker.create_node(
            name=f"{config.source_name}_{config.target_zone.value}_source",
            node_type="dataset",
            metadata={
                "zone": self._get_source_zone(config.target_zone).value,
                "record_count": len(source_records),
            },
        )
        
        # Create output node
        output_node = self.lineage_tracker.create_node(
            name=f"{config.source_name}_{config.target_zone.value}_output",
            node_type="dataset",
            metadata={
                "zone": config.target_zone.value,
                "record_count": len(output_records),
                "quality_score": quality_report.overall_score,
            },
        )
        
        # Record transformation
        self.lineage_tracker.record_transformation(
            source_nodes=[source_node],
            target_node=output_node,
            transformation_type=TransformationType.CLEANSE
            if config.target_zone == DataZone.BRONZE
            else TransformationType.TRANSFORM,
            transformation_logic=", ".join(config.transformations),
            metadata={
                "job_id": config.job_id,
                "load_type": config.load_type.value,
                "quality_status": quality_report.overall_status.value,
            },
        )
        
        return output_node.node_id
    
    def _write_to_zone(
        self,
        zone: DataZone,
        dataset_name: str,
        records: list[dict[str, Any]],
        load_type: LoadType,
    ) -> None:
        """Write records to target zone."""
        if dataset_name not in self.zones[zone]:
            self.zones[zone][dataset_name] = []
        
        if load_type == LoadType.FULL:
            self.zones[zone][dataset_name] = records
        elif load_type == LoadType.APPEND:
            self.zones[zone][dataset_name].extend(records)
        elif load_type == LoadType.MERGE:
            # Upsert by ID
            existing_ids = {
                r.get("id"): i
                for i, r in enumerate(self.zones[zone][dataset_name])
            }
            for record in records:
                record_id = record.get("id")
                if record_id in existing_ids:
                    self.zones[zone][dataset_name][existing_ids[record_id]] = record
                else:
                    self.zones[zone][dataset_name].append(record)
        elif load_type == LoadType.INCREMENTAL:
            # Append only new records
            existing_ids = {
                r.get("id") for r in self.zones[zone][dataset_name]
            }
            new_records = [
                r for r in records if r.get("id") not in existing_ids
            ]
            self.zones[zone][dataset_name].extend(new_records)
    
    # -------------------------------------------------------------------------
    # Standard Transformations
    # -------------------------------------------------------------------------
    
    def _transform_deduplicate(
        self,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Remove duplicate records."""
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        
        for record in records:
            record_id = str(record.get("id", ""))
            if record_id not in seen:
                seen.add(record_id)
                unique.append(record)
        
        return unique
    
    def _transform_standardize_dates(
        self,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Convert dates to ISO 8601 format."""
        from datetime import datetime
        
        date_fields = ["date", "created_at", "updated_at", "birth_date",
                       "admission_date", "discharge_date"]
        
        for record in records:
            for field in date_fields:
                if field in record and record[field]:
                    value = record[field]
                    # Try common date formats
                    for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d"]:
                        try:
                            if isinstance(value, str):
                                dt = datetime.strptime(value, fmt)
                                record[field] = dt.strftime("%Y-%m-%d")
                                break
                        except ValueError:
                            continue
        
        return records
    
    def _transform_uppercase_codes(
        self,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Standardize medical codes to uppercase."""
        code_fields = ["diagnosis_code", "procedure_code", "icd10_code",
                       "cpt_code", "ndc_code", "loinc_code"]
        
        for record in records:
            for field in code_fields:
                if field in record and record[field]:
                    record[field] = str(record[field]).upper().strip()
        
        return records
    
    def _transform_trim_strings(
        self,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Trim whitespace from string fields."""
        for record in records:
            for key, value in record.items():
                if isinstance(value, str):
                    record[key] = value.strip()
        
        return records
    
    def _transform_null_handling(
        self,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Standardize null/empty value representation."""
        null_values = ["", "NULL", "null", "N/A", "n/a", "NA", "None", "none"]
        
        for record in records:
            for key, value in record.items():
                if isinstance(value, str) and value in null_values:
                    record[key] = None
        
        return records
    
    def _transform_add_metadata(
        self,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Add processing metadata fields."""
        now = datetime.utcnow().isoformat()
        
        for record in records:
            record["_processed_at"] = now
            record["_version"] = 1
        
        return records
    
    # -------------------------------------------------------------------------
    # Zone Access
    # -------------------------------------------------------------------------
    
    def get_zone_data(
        self,
        zone: DataZone,
        dataset_name: str,
    ) -> list[dict[str, Any]]:
        """Get data from a specific zone."""
        return self.zones.get(zone, {}).get(dataset_name, [])
    
    def get_quarantined(
        self,
        job_id: str,
    ) -> list[dict[str, Any]]:
        """Get quarantined records for a job."""
        return self.quarantine.get(job_id, [])
    
    def promote_zone(
        self,
        dataset_name: str,
        from_zone: DataZone,
        to_zone: DataZone,
        job_config: ETLJobConfig | None = None,
    ) -> ETLJobResult:
        """
        Promote data from one zone to the next.
        
        Args:
            dataset_name: Name of the dataset
            from_zone: Source zone
            to_zone: Target zone
            job_config: Optional job configuration
            
        Returns:
            Job execution result
        """
        source_data = self.get_zone_data(from_zone, dataset_name)
        
        if not source_data:
            raise ValueError(f"No data found in {from_zone.value} for {dataset_name}")
        
        config = job_config or ETLJobConfig(
            job_id=f"promote_{dataset_name}_{from_zone.value}_{to_zone.value}",
            source_name=dataset_name,
            target_zone=to_zone,
            load_type=LoadType.FULL,
            transformations=["deduplicate", "trim_strings", "add_metadata"],
        )
        
        return self.run_job(config, source_data)
