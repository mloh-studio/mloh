output "sp_application_id" {
  description = "Pass to the bundle as --var sp_application_id=... and to the producer as ZEROBUS_CLIENT_ID."
  value       = databricks_service_principal.zerobus_writer.application_id
}

output "sp_client_secret" {
  description = "OAuth secret for the local producer (ZEROBUS_CLIENT_SECRET). Read with: terraform output -raw sp_client_secret"
  value       = databricks_service_principal_secret.zerobus_writer.secret
  sensitive   = true
}
