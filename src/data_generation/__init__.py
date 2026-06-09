"""Deterministic synthetic dataset generation for Treasury pipeline demos."""

from .generator import (
    DataGenerationConfig,
    GeneratedDatasetManifest,
    generate_demo_datasets,
)

__all__ = [
    "DataGenerationConfig",
    "GeneratedDatasetManifest",
    "generate_demo_datasets",
]
