import os
import subprocess
import tempfile

from splitgraph.ingestion.dbt.utils import (
    compile_dbt_manifest,
    patch_dbt_project_sources,
    prepare_git_repo,
)
from splitgraph.utils.yaml import safe_load

_REPO_PATH = "https://github.com/splitgraph/jaffle_shop_archive"


def test_dbt_repo_clone_patch():
    # Test a couple dbt helper utils by cloning an actual Git repository with the sample
    # project that we rescued from dbt's GitHub and running the patcher there.

    with tempfile.TemporaryDirectory() as tmp_dir:
        # demo/master actually has a declared source
        prepare_git_repo(_REPO_PATH, target_path=tmp_dir, ref="demo/master")

        assert "models" in os.listdir(tmp_dir)
        assert "dbt_project.yml" in os.listdir(tmp_dir)

        patch_dbt_project_sources(tmp_dir, "new_patched_schema")

        assert (
            subprocess.check_output(["git", "status", "--short"], cwd=tmp_dir).decode().strip()
            == "M models/staging/jaffle_shop/jaffle_shop.yml"
        )

        with open(os.path.join(tmp_dir, "models/staging/jaffle_shop/jaffle_shop.yml"), "r") as f:
            assert safe_load(f)["sources"][0]["schema"] == "new_patched_schema"

        patch_dbt_project_sources(
            tmp_dir,
            source_schema_map={
                "nonexistent_source": "other_schema",
                "raw_jaffle_shop": "jaffle_shop_schema",
            },
            default_schema="default_schema",
        )

        with open(os.path.join(tmp_dir, "models/staging/jaffle_shop/jaffle_shop.yml"), "r") as f:
            assert safe_load(f)["sources"][0]["schema"] == "jaffle_shop_schema"


def test_dbt_repo_build_manifest(local_engine_empty):
    manifest = compile_dbt_manifest(
        local_engine_empty, _REPO_PATH, repository_ref="sg-integration-test"
    )
    # Quickly check some entries in the manifest

    assert sorted(manifest.keys()) == [
        "child_map",
        "disabled",
        "docs",
        "exposures",
        "macros",
        "metadata",
        "metrics",
        "nodes",
        "parent_map",
        "selectors",
        "sources",
    ]
    assert sorted([n["name"] for n in manifest["nodes"].values()]) == [
        "accepted_values_fct_orders_status__placed__shipped__completed__return_pending__returned",
        "accepted_values_stg_jaffle_shop__orders_status__placed__shipped__completed__return_pending__returned",
        "accepted_values_stg_jaffle_shop__payments_payment_method__credit_card__coupon__bank_transfer__gift_card",
        "customer_orders",
        "customer_payments",
        "dim_customers",
        "fct_orders",
        "not_null_dim_customers_customer_id",
        "not_null_fct_orders_amount",
        "not_null_fct_orders_bank_transfer_amount",
        "not_null_fct_orders_coupon_amount",
        "not_null_fct_orders_credit_card_amount",
        "not_null_fct_orders_customer_id",
        "not_null_fct_orders_gift_card_amount",
        "not_null_fct_orders_order_id",
        "not_null_stg_jaffle_shop__customers_customer_id",
        "not_null_stg_jaffle_shop__orders_order_id",
        "not_null_stg_jaffle_shop__payments_payment_id",
        "order_payments",
        "relationships_fct_orders_customer_id__customer_id__ref_dim_customers_",
        "source_accepted_values_raw_jaffle_shop_orders_orders_status__placed__shipped__completed__return_pending__returned",
        "source_accepted_values_raw_jaffle_shop_orders_payments_payment_method__credit_card__coupon__bank_transfer__gift_card",
        "source_not_null_raw_jaffle_shop_customers_customers_id",
        "source_not_null_raw_jaffle_shop_orders_orders_id",
        "source_not_null_raw_jaffle_shop_orders_payments_id",
        "source_unique_raw_jaffle_shop_customers_customers_id",
        "source_unique_raw_jaffle_shop_orders_orders_id",
        "source_unique_raw_jaffle_shop_orders_payments_id",
        "stg_jaffle_shop__customers",
        "stg_jaffle_shop__orders",
        "stg_jaffle_shop__payments",
        "stg_stripe__payments",
        "unique_dim_customers_customer_id",
        "unique_fct_orders_order_id",
        "unique_stg_jaffle_shop__customers_customer_id",
        "unique_stg_jaffle_shop__orders_order_id",
        "unique_stg_jaffle_shop__payments_payment_id",
    ]

    assert sorted(
        [
            n["name"]
            for n in manifest["nodes"].values()
            if n["config"]["materialized"] == "table" and n["resource_type"] == "model"
        ]
    ) == [
        "customer_orders",
        "customer_payments",
        "dim_customers",
        "fct_orders",
        "order_payments",
    ]
