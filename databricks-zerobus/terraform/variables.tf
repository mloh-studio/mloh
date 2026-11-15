variable "databricks_profile" {
  description = "Profile in ~/.databrickscfg for the target workspace."
  type        = string
  default     = "DEFAULT"
}

variable "catalog" {
  description = "Existing catalog the bundle creates its schema in (must match bundle var `catalog`)."
  type        = string
  default     = "workspace"
}

variable "service_principal_name" {
  type    = string
  default = "zerobus-demo-writer"
}
