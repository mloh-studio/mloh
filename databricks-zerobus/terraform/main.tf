# Bundles can't create service principals, so that part lives here.
# Schema grants live with the schema in ../resources/schema.yml.

terraform {
  required_version = ">= 1.6"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.80.0"
    }
  }
}

provider "databricks" {
  profile = var.databricks_profile
}

resource "databricks_service_principal" "zerobus_writer" {
  display_name = var.service_principal_name
}

resource "databricks_service_principal_secret" "zerobus_writer" {
  service_principal_id = databricks_service_principal.zerobus_writer.id
}

resource "databricks_grant" "catalog_use" {
  catalog    = var.catalog
  principal  = databricks_service_principal.zerobus_writer.application_id
  privileges = ["USE_CATALOG"]
}
