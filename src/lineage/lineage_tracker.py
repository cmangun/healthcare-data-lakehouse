"""
Healthcare Data Lineage Tracking

Comprehensive data lineage for regulated healthcare environments:
- Full provenance tracking (Bronze → Silver → Gold → Platinum)
- Column-level lineage
- Impact analysis
- Audit-ready documentation
- FDA 21 CFR Part 11 compliance
"""

from __future__ import annotations

import hashlib
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class DataZone(str, Enum):
    """Data lakehouse zones (medallion architecture)."""
    
    LANDING = "landing"      # Raw ingestion
    BRONZE = "bronze"        # Raw, immutable data
    SILVER = "silver"        # Cleansed, conformed
    GOLD = "gold"            # Business-level aggregates
    PLATINUM = "platinum"    # ML-ready features


class TransformationType(str, Enum):
    """Types of data transformations."""
    
    INGESTION = "ingestion"
    CLEANING = "cleaning"
    DEDUPLICATION = "deduplication"
    STANDARDIZATION = "standardization"
    AGGREGATION = "aggregation"
    JOINING = "joining"
    FILTERING = "filtering"
    ENRICHMENT = "enrichment"
    ANONYMIZATION = "anonymization"
    FEATURE_ENGINEERING = "feature_engineering"


class QualityStatus(str, Enum):
    """Data quality status."""
    
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


@dataclass
class DataAsset:
    """A data asset in the lakehouse."""
    
    asset_id: str
    name: str
    zone: DataZone
    location: str  # URI to data location
    format: str  # parquet, delta, csv, etc.
    schema_version: str
    row_count: int | None = None
    column_count: int | None = None
    size_bytes: int | None = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = "system"
    tags: dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "name": self.name,
            "zone": self.zone.value,
            "location": self.location,
            "format": self.format,
            "schema_version": self.schema_version,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "tags": self.tags,
        }


@dataclass
class ColumnLineage:
    """Column-level lineage information."""
    
    target_column: str
    source_columns: list[tuple[str, str]]  # (asset_id, column_name)
    transformation: str
    logic: str | None = None
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "target_column": self.target_column,
            "source_columns": [
                {"asset_id": a, "column": c} for a, c in self.source_columns
            ],
            "transformation": self.transformation,
            "logic": self.logic,
        }


@dataclass
class DataQualityCheck:
    """A data quality check result."""
    
    check_id: str
    check_name: str
    check_type: str  # completeness, accuracy, consistency, etc.
    status: QualityStatus
    expected_value: Any
    actual_value: Any
    threshold: float | None = None
    error_message: str | None = None
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "check_id": self.check_id,
            "check_name": self.check_name,
            "check_type": self.check_type,
            "status": self.status.value,
            "expected_value": self.expected_value,
            "actual_value": self.actual_value,
            "threshold": self.threshold,
            "error_message": self.error_message,
        }


@dataclass
class TransformationStep:
    """A step in a data transformation pipeline."""
    
    step_id: str
    step_name: str
    transformation_type: TransformationType
    input_assets: list[str]  # asset_ids
    output_asset: str  # asset_id
    column_lineage: list[ColumnLineage]
    quality_checks: list[DataQualityCheck]
    started_at: datetime
    completed_at: datetime
    duration_seconds: float
    records_processed: int
    records_output: int
    error: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "step_name": self.step_name,
            "transformation_type": self.transformation_type.value,
            "input_assets": self.input_assets,
            "output_asset": self.output_asset,
            "column_lineage": [c.to_dict() for c in self.column_lineage],
            "quality_checks": [q.to_dict() for q in self.quality_checks],
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "records_processed": self.records_processed,
            "records_output": self.records_output,
            "error": self.error,
            "parameters": self.parameters,
        }


@dataclass
class PipelineRun:
    """A complete pipeline execution."""
    
    run_id: str
    pipeline_name: str
    pipeline_version: str
    steps: list[TransformationStep]
    started_at: datetime
    completed_at: datetime | None = None
    status: str = "running"
    triggered_by: str = "schedule"
    error: str | None = None
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "steps": [s.to_dict() for s in self.steps],
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "triggered_by": self.triggered_by,
            "error": self.error,
        }


class LineageConfig(BaseModel):
    """Configuration for lineage tracking."""
    
    enable_column_lineage: bool = True
    enable_quality_checks: bool = True
    retention_days: int = Field(default=2555, ge=1)  # 7 years for HIPAA
    hash_sensitive_values: bool = True
    track_row_counts: bool = True


class LineageTracker:
    """
    Production data lineage tracker for healthcare.
    
    Features:
    - Full data provenance
    - Column-level lineage
    - Quality check integration
    - Impact analysis
    - Audit-ready exports
    - Graph traversal
    """
    
    def __init__(self, config: LineageConfig | None = None):
        self.config = config or LineageConfig()
        self._assets: dict[str, DataAsset] = {}
        self._pipeline_runs: dict[str, PipelineRun] = {}
        self._lineage_graph: dict[str, set[str]] = defaultdict(set)  # downstream dependencies
        self._reverse_graph: dict[str, set[str]] = defaultdict(set)  # upstream dependencies
    
    def register_asset(
        self,
        name: str,
        zone: DataZone,
        location: str,
        format: str = "parquet",
        schema_version: str = "1.0.0",
        **kwargs: Any,
    ) -> DataAsset:
        """Register a new data asset."""
        asset_id = self._generate_asset_id(name, zone, schema_version)
        
        asset = DataAsset(
            asset_id=asset_id,
            name=name,
            zone=zone,
            location=location,
            format=format,
            schema_version=schema_version,
            **kwargs,
        )
        
        self._assets[asset_id] = asset
        
        logger.info(
            "asset_registered",
            asset_id=asset_id,
            name=name,
            zone=zone.value,
        )
        
        return asset
    
    def record_transformation(
        self,
        step_name: str,
        transformation_type: TransformationType,
        input_asset_ids: list[str],
        output_asset_id: str,
        column_lineage: list[ColumnLineage] | None = None,
        quality_checks: list[DataQualityCheck] | None = None,
        records_processed: int = 0,
        records_output: int = 0,
        parameters: dict[str, Any] | None = None,
        pipeline_run_id: str | None = None,
    ) -> TransformationStep:
        """
        Record a data transformation step.
        
        Args:
            step_name: Human-readable step name
            transformation_type: Type of transformation
            input_asset_ids: Source asset IDs
            output_asset_id: Target asset ID
            column_lineage: Column-level lineage
            quality_checks: Quality check results
            records_processed: Input record count
            records_output: Output record count
            parameters: Transformation parameters
            pipeline_run_id: Parent pipeline run
        
        Returns:
            TransformationStep record
        """
        now = datetime.utcnow()
        
        step = TransformationStep(
            step_id=str(uuid.uuid4()),
            step_name=step_name,
            transformation_type=transformation_type,
            input_assets=input_asset_ids,
            output_asset=output_asset_id,
            column_lineage=column_lineage or [],
            quality_checks=quality_checks or [],
            started_at=now,
            completed_at=now,
            duration_seconds=0,
            records_processed=records_processed,
            records_output=records_output,
            parameters=parameters or {},
        )
        
        # Update lineage graph
        for input_id in input_asset_ids:
            self._lineage_graph[input_id].add(output_asset_id)
            self._reverse_graph[output_asset_id].add(input_id)
        
        # Add to pipeline run if specified
        if pipeline_run_id and pipeline_run_id in self._pipeline_runs:
            self._pipeline_runs[pipeline_run_id].steps.append(step)
        
        logger.info(
            "transformation_recorded",
            step_id=step.step_id,
            step_name=step_name,
            transformation_type=transformation_type.value,
            input_count=len(input_asset_ids),
        )
        
        return step
    
    def start_pipeline_run(
        self,
        pipeline_name: str,
        pipeline_version: str = "1.0.0",
        triggered_by: str = "schedule",
    ) -> PipelineRun:
        """Start a new pipeline run."""
        run = PipelineRun(
            run_id=str(uuid.uuid4()),
            pipeline_name=pipeline_name,
            pipeline_version=pipeline_version,
            steps=[],
            started_at=datetime.utcnow(),
            triggered_by=triggered_by,
        )
        
        self._pipeline_runs[run.run_id] = run
        
        logger.info(
            "pipeline_run_started",
            run_id=run.run_id,
            pipeline_name=pipeline_name,
        )
        
        return run
    
    def complete_pipeline_run(
        self,
        run_id: str,
        status: str = "success",
        error: str | None = None,
    ) -> PipelineRun:
        """Complete a pipeline run."""
        run = self._pipeline_runs.get(run_id)
        if not run:
            raise ValueError(f"Pipeline run not found: {run_id}")
        
        run.completed_at = datetime.utcnow()
        run.status = status
        run.error = error
        
        logger.info(
            "pipeline_run_completed",
            run_id=run_id,
            status=status,
            num_steps=len(run.steps),
        )
        
        return run
    
    def get_upstream_lineage(
        self,
        asset_id: str,
        max_depth: int = 10,
    ) -> list[str]:
        """
        Get all upstream dependencies of an asset.
        
        Args:
            asset_id: Asset to trace
            max_depth: Maximum traversal depth
        
        Returns:
            List of upstream asset IDs
        """
        visited = set()
        result = []
        
        def traverse(current_id: str, depth: int) -> None:
            if depth > max_depth or current_id in visited:
                return
            
            visited.add(current_id)
            
            for upstream_id in self._reverse_graph.get(current_id, []):
                result.append(upstream_id)
                traverse(upstream_id, depth + 1)
        
        traverse(asset_id, 0)
        return result
    
    def get_downstream_impact(
        self,
        asset_id: str,
        max_depth: int = 10,
    ) -> list[str]:
        """
        Get all downstream assets impacted by changes to an asset.
        
        Args:
            asset_id: Asset to analyze
            max_depth: Maximum traversal depth
        
        Returns:
            List of downstream asset IDs
        """
        visited = set()
        result = []
        
        def traverse(current_id: str, depth: int) -> None:
            if depth > max_depth or current_id in visited:
                return
            
            visited.add(current_id)
            
            for downstream_id in self._lineage_graph.get(current_id, []):
                result.append(downstream_id)
                traverse(downstream_id, depth + 1)
        
        traverse(asset_id, 0)
        return result
    
    def generate_lineage_report(
        self,
        asset_id: str,
    ) -> dict[str, Any]:
        """
        Generate a comprehensive lineage report for an asset.
        
        Suitable for regulatory documentation.
        """
        asset = self._assets.get(asset_id)
        if not asset:
            raise ValueError(f"Asset not found: {asset_id}")
        
        upstream = self.get_upstream_lineage(asset_id)
        downstream = self.get_downstream_impact(asset_id)
        
        # Get upstream assets details
        upstream_assets = [
            self._assets[uid].to_dict()
            for uid in upstream
            if uid in self._assets
        ]
        
        # Get downstream assets details
        downstream_assets = [
            self._assets[did].to_dict()
            for did in downstream
            if did in self._assets
        ]
        
        return {
            "report_generated_at": datetime.utcnow().isoformat(),
            "asset": asset.to_dict(),
            "upstream_lineage": {
                "count": len(upstream),
                "assets": upstream_assets,
            },
            "downstream_impact": {
                "count": len(downstream),
                "assets": downstream_assets,
            },
            "data_zones_traversed": list(set(
                self._assets[aid].zone.value
                for aid in upstream + downstream + [asset_id]
                if aid in self._assets
            )),
        }
    
    def export_for_audit(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, Any]:
        """
        Export lineage data for regulatory audit.
        
        Returns all assets, transformations, and pipeline runs
        within the specified date range.
        """
        assets = list(self._assets.values())
        runs = list(self._pipeline_runs.values())
        
        # Filter by date if specified
        if start_date:
            assets = [a for a in assets if a.created_at >= start_date]
            runs = [r for r in runs if r.started_at >= start_date]
        
        if end_date:
            assets = [a for a in assets if a.created_at <= end_date]
            runs = [r for r in runs if r.started_at <= end_date]
        
        return {
            "export_timestamp": datetime.utcnow().isoformat(),
            "date_range": {
                "start": start_date.isoformat() if start_date else None,
                "end": end_date.isoformat() if end_date else None,
            },
            "summary": {
                "total_assets": len(assets),
                "total_pipeline_runs": len(runs),
                "total_transformations": sum(len(r.steps) for r in runs),
            },
            "assets": [a.to_dict() for a in assets],
            "pipeline_runs": [r.to_dict() for r in runs],
        }
    
    def _generate_asset_id(
        self,
        name: str,
        zone: DataZone,
        schema_version: str,
    ) -> str:
        """Generate unique asset ID."""
        content = f"{name}:{zone.value}:{schema_version}"
        return f"asset_{hashlib.sha256(content.encode()).hexdigest()[:16]}"
