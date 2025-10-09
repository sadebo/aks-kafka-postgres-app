variable "location" {
  description = "Azure region"
  default     = "eastus"
}

variable "rg_name" {
  description = "Resource group name"
  default     = "rg-kafka-postgres-demo"
}

variable "aks_name" {
  description = "AKS cluster name"
  default     = "aks-kafka-postgres"
}

variable "node_count" {
  default = 2
}

variable "node_size" {
  default = "Standard_B2s"
}
