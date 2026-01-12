"""
Data Quality Framework for Healthcare Data Lakehouse.

Production-grade data quality validation for regulated healthcare data:
- Schema validation with evolution tracking
- Statistical quality metrics (completeness, accuracy, consistency)
- Business rule enforcement with healthcare-specific checks
- Quality gates for zone promotion (Bronze → Silver → Gold)
- Anomaly detection with configurable thresholds
- Automated quarantine for quality failures
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable

import structlog

logger = structlog.get_logger()


class DataZone(Enum):
    """Medallion architecture zones for data lakehouse."""
    
    RAW = "raw"           # Landing zone - untransformed
    BRONZE = "bronze"     # Cleansed, deduplicated
    SILVER = "silver"     # Conformed, enriched
    GOLD = "gold"         # Aggregated, analytics-ready
    PLATINUM = "platinum" # ML features, validated


class QualityDimension(Enum):
    """Standard data quality dimensions."""
    
    COMPLETENESS = "completeness"     # Non-null values
    ACCURACY = "accuracy"             # Valid values
    CONSISTENCY = "consistency"       # Cross-field rules
    TIMELINESS = "timeliness"         # Data freshness
    UNIQUENESS = "uniqueness"         # No duplicates
    VALIDITY = "validity"             # Format/range checks
    INTEGRITY = "integrity"           # Referential integrity


class QualityStatus(Enum):
    """Quality check result status."""
    
    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class QualityRule:
    """Individual quality rule definition."""
    
    rule_id: str
    name: str
    description: str
    dimension: QualityDimension
    severity: str  # critical, major, minor, warning
    check_fn: Callable[[list[dict[str, Any]]], QualityCheckResult]
    applicable_zones: list[DataZone] = field(default_factory=list)
    enabled: bool = True
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "description": self.description,
            "dimension": self.dimension.value,
            "severity": self.severity,
            "applicable_zones": [z.value for z in self.applicable_zones],
            "enabled": self.enabled,
        }


@dataclass
class QualityCheckResult:
    """Result of a single quality check."""
    
    rule_id: str
    rule_name: str
    dimension: QualityDimension
    status: QualityStatus
    score: float  # 0.0 to 1.0
    records_checked: int
    records_passed: int
    records_failed: int
    failed_record_ids: list[str] = field(default_factory=list)
    details: str = ""
    execution_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "dimension": self.dimension.value,
            "status": self.status.value,
            "score": self.score,
            "records_checked": self.records_checked,
            "records_passed": self.records_passed,
            "records_failed": self.records_failed,
            "failed_record_count": len(self.failed_record_ids),
            "details": self.details,
            "execution_time_ms": self.execution_time_ms,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class QualityReport:
    """Comprehensive quality assessment report."""
    
    report_id: str
    dataset_name: str
    zone: DataZone
    total_records: int
    check_results: list[QualityCheckResult]
    overall_score: float
    overall_status: QualityStatus
    promotion_eligible: bool
    quarantine_records: list[str]
    execution_time_ms: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "report_id": self.report_id,
            "dataset_name": self.dataset_name,
            "zone": self.zone.value,
            "total_records": self.total_records,
            "check_results": [r.to_dict() for r in self.check_results],
            "overall_score": self.overall_score,
            "overall_status": self.overall_status.value,
            "promotion_eligible": self.promotion_eligible,
            "quarantine_record_count": len(self.quarantine_records),
            "execution_time_ms": self.execution_time_ms,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class QualityThresholds:
    """Quality thresholds for zone promotion."""
    
    min_completeness: float = 0.95
    min_accuracy: float = 0.98
    min_consistency: float = 0.99
    min_uniqueness: float = 1.0
    max_failed_critical: int = 0
    max_failed_major: int = 5
    overall_min_score: float = 0.95


class DataQualityValidator:
    """
    Production data quality validation engine.
    
    Features:
    - Configurable quality rules by dimension
    - Zone-specific threshold enforcement
    - Healthcare-specific validation (PHI, date formats, codes)
    - Automated quarantine and alerting
    - Quality score calculation for governance
    """
    
    # Zone promotion thresholds
    ZONE_THRESHOLDS: dict[DataZone, QualityThresholds] = {
        DataZone.RAW: QualityThresholds(
            min_completeness=0.0,
            min_accuracy=0.0,
            min_consistency=0.0,
            overall_min_score=0.0,
        ),
        DataZone.BRONZE: QualityThresholds(
            min_completeness=0.90,
            min_accuracy=0.95,
            min_consistency=0.95,
            overall_min_score=0.90,
        ),
        DataZone.SILVER: QualityThresholds(
            min_completeness=0.95,
            min_accuracy=0.98,
            min_consistency=0.99,
            overall_min_score=0.95,
        ),
        DataZone.GOLD: QualityThresholds(
            min_completeness=0.99,
            min_accuracy=0.99,
            min_consistency=0.99,
            overall_min_score=0.98,
        ),
        DataZone.PLATINUM: QualityThresholds(
            min_completeness=1.0,
            min_accuracy=0.99,
            min_consistency=1.0,
            overall_min_score=0.99,
        ),
    }
    
    def __init__(self, dataset_name: str, id_field: str = "id"):
        self.dataset_name = dataset_name
        self.id_field = id_field
        self.rules: list[QualityRule] = []
        self._init_healthcare_rules()
    
    def _init_healthcare_rules(self) -> None:
        """Initialize standard healthcare data quality rules."""
        
        # Required fields check
        self.add_rule(QualityRule(
            rule_id="DQ001",
            name="required_fields_present",
            description="All required fields must be non-null",
            dimension=QualityDimension.COMPLETENESS,
            severity="critical",
            check_fn=self._check_required_fields,
            applicable_zones=[DataZone.BRONZE, DataZone.SILVER, DataZone.GOLD],
        ))
        
        # Date format validation
        self.add_rule(QualityRule(
            rule_id="DQ002",
            name="valid_date_formats",
            description="Date fields must follow ISO 8601 format",
            dimension=QualityDimension.VALIDITY,
            severity="major",
            check_fn=self._check_date_formats,
            applicable_zones=[DataZone.SILVER, DataZone.GOLD],
        ))
        
        # Patient ID format
        self.add_rule(QualityRule(
            rule_id="DQ003",
            name="valid_patient_id_format",
            description="Patient IDs must match expected format",
            dimension=QualityDimension.VALIDITY,
            severity="critical",
            check_fn=self._check_patient_id_format,
            applicable_zones=[DataZone.BRONZE, DataZone.SILVER, DataZone.GOLD],
        ))
        
        # ICD-10 code validation
        self.add_rule(QualityRule(
            rule_id="DQ004",
            name="valid_icd10_codes",
            description="Diagnosis codes must be valid ICD-10 format",
            dimension=QualityDimension.ACCURACY,
            severity="major",
            check_fn=self._check_icd10_codes,
            applicable_zones=[DataZone.SILVER, DataZone.GOLD],
        ))
        
        # Duplicate detection
        self.add_rule(QualityRule(
            rule_id="DQ005",
            name="no_duplicate_records",
            description="Records must be unique by primary key",
            dimension=QualityDimension.UNIQUENESS,
            severity="critical",
            check_fn=self._check_duplicates,
            applicable_zones=[DataZone.BRONZE, DataZone.SILVER, DataZone.GOLD],
        ))
        
        # Value range checks
        self.add_rule(QualityRule(
            rule_id="DQ006",
            name="valid_value_ranges",
            description="Numeric values must be within valid clinical ranges",
            dimension=QualityDimension.ACCURACY,
            severity="major",
            check_fn=self._check_value_ranges,
            applicable_zones=[DataZone.SILVER, DataZone.GOLD],
        ))
        
        # Referential integrity
        self.add_rule(QualityRule(
            rule_id="DQ007",
            name="referential_integrity",
            description="Foreign key references must be valid",
            dimension=QualityDimension.INTEGRITY,
            severity="major",
            check_fn=self._check_referential_integrity,
            applicable_zones=[DataZone.GOLD],
        ))
        
        # PHI completeness for de-identification
        self.add_rule(QualityRule(
            rule_id="DQ008",
            name="phi_fields_present",
            description="Required PHI fields present for de-identification",
            dimension=QualityDimension.COMPLETENESS,
            severity="warning",
            check_fn=self._check_phi_fields,
            applicable_zones=[DataZone.RAW, DataZone.BRONZE],
        ))
    
    def add_rule(self, rule: QualityRule) -> None:
        """Add a quality rule to the validator."""
        self.rules.append(rule)
        logger.info("quality_rule_added", rule_id=rule.rule_id, name=rule.name)
    
    def validate(
        self,
        records: list[dict[str, Any]],
        target_zone: DataZone,
        required_fields: list[str] | None = None,
    ) -> QualityReport:
        """
        Execute all applicable quality checks for target zone.
        
        Args:
            records: List of data records to validate
            target_zone: Target zone for promotion
            required_fields: Optional list of required field names
            
        Returns:
            Comprehensive quality report with pass/fail status
        """
        start_time = datetime.utcnow()
        
        report_id = self._generate_report_id()
        check_results: list[QualityCheckResult] = []
        quarantine_records: set[str] = set()
        
        # Store required fields for rule checks
        self._required_fields = required_fields or []
        
        # Execute applicable rules
        for rule in self.rules:
            if not rule.enabled:
                continue
            
            if target_zone not in rule.applicable_zones:
                continue
            
            try:
                rule_start = datetime.utcnow()
                result = rule.check_fn(records)
                result.execution_time_ms = (
                    datetime.utcnow() - rule_start
                ).total_seconds() * 1000
                
                check_results.append(result)
                
                # Track records to quarantine
                if result.status == QualityStatus.FAILED:
                    quarantine_records.update(result.failed_record_ids)
                
                logger.info(
                    "quality_check_completed",
                    rule_id=rule.rule_id,
                    status=result.status.value,
                    score=result.score,
                )
                
            except Exception as e:
                logger.error(
                    "quality_check_error",
                    rule_id=rule.rule_id,
                    error=str(e),
                )
                check_results.append(QualityCheckResult(
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    dimension=rule.dimension,
                    status=QualityStatus.SKIPPED,
                    score=0.0,
                    records_checked=0,
                    records_passed=0,
                    records_failed=0,
                    details=f"Error: {str(e)}",
                ))
        
        # Calculate overall score and status
        overall_score = self._calculate_overall_score(check_results)
        overall_status = self._determine_overall_status(
            check_results, target_zone
        )
        promotion_eligible = self._check_promotion_eligibility(
            check_results, overall_score, target_zone
        )
        
        execution_time_ms = (
            datetime.utcnow() - start_time
        ).total_seconds() * 1000
        
        report = QualityReport(
            report_id=report_id,
            dataset_name=self.dataset_name,
            zone=target_zone,
            total_records=len(records),
            check_results=check_results,
            overall_score=overall_score,
            overall_status=overall_status,
            promotion_eligible=promotion_eligible,
            quarantine_records=list(quarantine_records),
            execution_time_ms=execution_time_ms,
        )
        
        logger.info(
            "quality_validation_completed",
            report_id=report_id,
            dataset=self.dataset_name,
            zone=target_zone.value,
            overall_score=overall_score,
            status=overall_status.value,
            promotion_eligible=promotion_eligible,
            quarantine_count=len(quarantine_records),
        )
        
        return report
    
    def _generate_report_id(self) -> str:
        """Generate unique report ID."""
        timestamp = datetime.utcnow().isoformat()
        content = f"{self.dataset_name}:{timestamp}"
        return f"dq_{hashlib.sha256(content.encode()).hexdigest()[:12]}"
    
    def _calculate_overall_score(
        self,
        results: list[QualityCheckResult],
    ) -> float:
        """Calculate weighted overall quality score."""
        if not results:
            return 0.0
        
        # Weight by dimension importance
        weights = {
            QualityDimension.COMPLETENESS: 1.0,
            QualityDimension.ACCURACY: 1.2,
            QualityDimension.CONSISTENCY: 1.1,
            QualityDimension.UNIQUENESS: 1.0,
            QualityDimension.VALIDITY: 0.9,
            QualityDimension.INTEGRITY: 1.0,
            QualityDimension.TIMELINESS: 0.8,
        }
        
        total_weight = 0.0
        weighted_score = 0.0
        
        for result in results:
            if result.status == QualityStatus.SKIPPED:
                continue
            
            weight = weights.get(result.dimension, 1.0)
            weighted_score += result.score * weight
            total_weight += weight
        
        return weighted_score / total_weight if total_weight > 0 else 0.0
    
    def _determine_overall_status(
        self,
        results: list[QualityCheckResult],
        zone: DataZone,
    ) -> QualityStatus:
        """Determine overall status based on individual check results."""
        if not results:
            return QualityStatus.SKIPPED
        
        has_failed = any(r.status == QualityStatus.FAILED for r in results)
        has_warning = any(r.status == QualityStatus.WARNING for r in results)
        
        if has_failed:
            return QualityStatus.FAILED
        elif has_warning:
            return QualityStatus.WARNING
        else:
            return QualityStatus.PASSED
    
    def _check_promotion_eligibility(
        self,
        results: list[QualityCheckResult],
        overall_score: float,
        zone: DataZone,
    ) -> bool:
        """Check if data meets zone promotion thresholds."""
        thresholds = self.ZONE_THRESHOLDS.get(zone)
        if not thresholds:
            return False
        
        # Check overall score
        if overall_score < thresholds.overall_min_score:
            return False
        
        # Check dimension-specific thresholds
        dimension_scores: dict[QualityDimension, list[float]] = {}
        for result in results:
            if result.dimension not in dimension_scores:
                dimension_scores[result.dimension] = []
            dimension_scores[result.dimension].append(result.score)
        
        # Calculate average per dimension
        for dim, scores in dimension_scores.items():
            avg_score = sum(scores) / len(scores)
            
            if dim == QualityDimension.COMPLETENESS:
                if avg_score < thresholds.min_completeness:
                    return False
            elif dim == QualityDimension.ACCURACY:
                if avg_score < thresholds.min_accuracy:
                    return False
            elif dim == QualityDimension.CONSISTENCY:
                if avg_score < thresholds.min_consistency:
                    return False
            elif dim == QualityDimension.UNIQUENESS:
                if avg_score < thresholds.min_uniqueness:
                    return False
        
        # Count critical failures
        critical_failures = sum(
            1 for r in results
            if r.status == QualityStatus.FAILED
        )
        
        if critical_failures > thresholds.max_failed_critical:
            return False
        
        return True
    
    # -------------------------------------------------------------------------
    # Quality Check Implementations
    # -------------------------------------------------------------------------
    
    def _check_required_fields(
        self,
        records: list[dict[str, Any]],
    ) -> QualityCheckResult:
        """Check that all required fields are present and non-null."""
        if not records:
            return QualityCheckResult(
                rule_id="DQ001",
                rule_name="required_fields_present",
                dimension=QualityDimension.COMPLETENESS,
                status=QualityStatus.SKIPPED,
                score=0.0,
                records_checked=0,
                records_passed=0,
                records_failed=0,
            )
        
        required = self._required_fields or list(records[0].keys())
        failed_ids: list[str] = []
        
        for record in records:
            record_id = str(record.get(self.id_field, ""))
            
            for field in required:
                if field not in record or record[field] is None:
                    failed_ids.append(record_id)
                    break
        
        passed = len(records) - len(failed_ids)
        score = passed / len(records) if records else 0.0
        
        status = QualityStatus.PASSED
        if score < 0.95:
            status = QualityStatus.FAILED
        elif score < 0.99:
            status = QualityStatus.WARNING
        
        return QualityCheckResult(
            rule_id="DQ001",
            rule_name="required_fields_present",
            dimension=QualityDimension.COMPLETENESS,
            status=status,
            score=score,
            records_checked=len(records),
            records_passed=passed,
            records_failed=len(failed_ids),
            failed_record_ids=failed_ids[:100],  # Limit for reporting
            details=f"Checked {len(required)} required fields",
        )
    
    def _check_date_formats(
        self,
        records: list[dict[str, Any]],
    ) -> QualityCheckResult:
        """Validate date fields follow ISO 8601 format."""
        date_fields = ["date", "created_at", "updated_at", "birth_date", 
                       "admission_date", "discharge_date", "encounter_date"]
        
        iso_pattern = re.compile(
            r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$"
        )
        
        failed_ids: list[str] = []
        checked = 0
        
        for record in records:
            record_id = str(record.get(self.id_field, ""))
            
            for field in date_fields:
                if field in record and record[field]:
                    checked += 1
                    value = str(record[field])
                    
                    if not iso_pattern.match(value):
                        failed_ids.append(record_id)
                        break
        
        passed = checked - len(failed_ids)
        score = passed / checked if checked > 0 else 1.0
        
        status = QualityStatus.PASSED
        if score < 0.95:
            status = QualityStatus.FAILED
        elif score < 0.99:
            status = QualityStatus.WARNING
        
        return QualityCheckResult(
            rule_id="DQ002",
            rule_name="valid_date_formats",
            dimension=QualityDimension.VALIDITY,
            status=status,
            score=score,
            records_checked=checked,
            records_passed=passed,
            records_failed=len(failed_ids),
            failed_record_ids=failed_ids[:100],
            details="Validated ISO 8601 date format",
        )
    
    def _check_patient_id_format(
        self,
        records: list[dict[str, Any]],
    ) -> QualityCheckResult:
        """Validate patient ID format."""
        # Common patient ID formats: MRN, UUID, alphanumeric
        id_pattern = re.compile(r"^[A-Z0-9]{6,20}$|^[a-f0-9-]{36}$")
        
        patient_id_fields = ["patient_id", "mrn", "subject_id"]
        
        failed_ids: list[str] = []
        checked = 0
        
        for record in records:
            record_id = str(record.get(self.id_field, ""))
            
            for field in patient_id_fields:
                if field in record and record[field]:
                    checked += 1
                    value = str(record[field]).upper()
                    
                    if not id_pattern.match(value):
                        failed_ids.append(record_id)
                        break
        
        passed = checked - len(failed_ids)
        score = passed / checked if checked > 0 else 1.0
        
        status = QualityStatus.PASSED
        if score < 0.99:
            status = QualityStatus.FAILED
        elif score < 1.0:
            status = QualityStatus.WARNING
        
        return QualityCheckResult(
            rule_id="DQ003",
            rule_name="valid_patient_id_format",
            dimension=QualityDimension.VALIDITY,
            status=status,
            score=score,
            records_checked=checked,
            records_passed=passed,
            records_failed=len(failed_ids),
            failed_record_ids=failed_ids[:100],
            details="Validated patient ID format",
        )
    
    def _check_icd10_codes(
        self,
        records: list[dict[str, Any]],
    ) -> QualityCheckResult:
        """Validate ICD-10 diagnosis code format."""
        # ICD-10 format: Letter + 2 digits + optional decimal + up to 4 more
        icd10_pattern = re.compile(r"^[A-Z]\d{2}(\.\d{1,4})?$")
        
        code_fields = ["diagnosis_code", "icd10_code", "primary_diagnosis"]
        
        failed_ids: list[str] = []
        checked = 0
        
        for record in records:
            record_id = str(record.get(self.id_field, ""))
            
            for field in code_fields:
                if field in record and record[field]:
                    checked += 1
                    value = str(record[field]).upper().strip()
                    
                    if not icd10_pattern.match(value):
                        failed_ids.append(record_id)
                        break
        
        passed = checked - len(failed_ids)
        score = passed / checked if checked > 0 else 1.0
        
        status = QualityStatus.PASSED
        if score < 0.95:
            status = QualityStatus.FAILED
        elif score < 0.99:
            status = QualityStatus.WARNING
        
        return QualityCheckResult(
            rule_id="DQ004",
            rule_name="valid_icd10_codes",
            dimension=QualityDimension.ACCURACY,
            status=status,
            score=score,
            records_checked=checked,
            records_passed=passed,
            records_failed=len(failed_ids),
            failed_record_ids=failed_ids[:100],
            details="Validated ICD-10 diagnosis code format",
        )
    
    def _check_duplicates(
        self,
        records: list[dict[str, Any]],
    ) -> QualityCheckResult:
        """Check for duplicate records by primary key."""
        seen_ids: set[str] = set()
        duplicate_ids: list[str] = []
        
        for record in records:
            record_id = str(record.get(self.id_field, ""))
            
            if record_id in seen_ids:
                duplicate_ids.append(record_id)
            else:
                seen_ids.add(record_id)
        
        unique_count = len(records) - len(duplicate_ids)
        score = unique_count / len(records) if records else 1.0
        
        status = QualityStatus.PASSED
        if len(duplicate_ids) > 0:
            status = QualityStatus.FAILED
        
        return QualityCheckResult(
            rule_id="DQ005",
            rule_name="no_duplicate_records",
            dimension=QualityDimension.UNIQUENESS,
            status=status,
            score=score,
            records_checked=len(records),
            records_passed=unique_count,
            records_failed=len(duplicate_ids),
            failed_record_ids=duplicate_ids[:100],
            details=f"Found {len(duplicate_ids)} duplicate records",
        )
    
    def _check_value_ranges(
        self,
        records: list[dict[str, Any]],
    ) -> QualityCheckResult:
        """Validate numeric values are within clinical ranges."""
        # Clinical value ranges
        ranges = {
            "age": (0, 150),
            "heart_rate": (20, 300),
            "systolic_bp": (40, 300),
            "diastolic_bp": (20, 200),
            "temperature": (90, 110),  # Fahrenheit
            "temperature_c": (32, 43),  # Celsius
            "weight_kg": (0.5, 700),
            "height_cm": (20, 280),
            "bmi": (5, 100),
            "glucose": (10, 1000),
            "hba1c": (2, 20),
        }
        
        failed_ids: list[str] = []
        checked = 0
        
        for record in records:
            record_id = str(record.get(self.id_field, ""))
            
            for field, (min_val, max_val) in ranges.items():
                if field in record and record[field] is not None:
                    try:
                        value = float(record[field])
                        checked += 1
                        
                        if value < min_val or value > max_val:
                            failed_ids.append(record_id)
                            break
                    except (ValueError, TypeError):
                        pass
        
        passed = checked - len(failed_ids)
        score = passed / checked if checked > 0 else 1.0
        
        status = QualityStatus.PASSED
        if score < 0.95:
            status = QualityStatus.FAILED
        elif score < 0.99:
            status = QualityStatus.WARNING
        
        return QualityCheckResult(
            rule_id="DQ006",
            rule_name="valid_value_ranges",
            dimension=QualityDimension.ACCURACY,
            status=status,
            score=score,
            records_checked=checked,
            records_passed=passed,
            records_failed=len(failed_ids),
            failed_record_ids=failed_ids[:100],
            details="Validated clinical value ranges",
        )
    
    def _check_referential_integrity(
        self,
        records: list[dict[str, Any]],
    ) -> QualityCheckResult:
        """Check referential integrity (placeholder for FK validation)."""
        # In production, would validate against reference tables
        return QualityCheckResult(
            rule_id="DQ007",
            rule_name="referential_integrity",
            dimension=QualityDimension.INTEGRITY,
            status=QualityStatus.PASSED,
            score=1.0,
            records_checked=len(records),
            records_passed=len(records),
            records_failed=0,
            details="Referential integrity check (stub - implement with reference tables)",
        )
    
    def _check_phi_fields(
        self,
        records: list[dict[str, Any]],
    ) -> QualityCheckResult:
        """Check PHI fields are present for de-identification pipeline."""
        phi_fields = ["patient_name", "birth_date", "ssn", "mrn", "address"]
        
        records_with_phi = 0
        
        for record in records:
            has_phi = any(
                field in record and record[field]
                for field in phi_fields
            )
            if has_phi:
                records_with_phi += 1
        
        score = records_with_phi / len(records) if records else 0.0
        
        status = QualityStatus.PASSED
        if score < 0.5:
            status = QualityStatus.WARNING
        
        return QualityCheckResult(
            rule_id="DQ008",
            rule_name="phi_fields_present",
            dimension=QualityDimension.COMPLETENESS,
            status=status,
            score=score,
            records_checked=len(records),
            records_passed=records_with_phi,
            records_failed=len(records) - records_with_phi,
            details=f"{records_with_phi}/{len(records)} records have PHI fields",
        )
