# ADLS Setup Guide

This guide helps you configure Azure Data Lake Storage Gen2 for your Polster project.

## Azure Data Lake Storage Gen2 Configuration

### Prerequisites
1. Azure Storage Account with Data Lake Storage Gen2 enabled
2. Storage Account Key (or SAS token with appropriate permissions)
3. Container created in your storage account

### Current Configuration
- Provider: adls
- Account Name: woningmarktintelx9
- Container: dna-platform-data

### Local Development (.env file)
Your `.env` file should contain:
```
STORAGE_BACKEND=adls
ADLS_ACCOUNT_NAME=woningmarktintelx9
ADLS_CONTAINER=dna-platform-data
ADLS_ACCOUNT_KEY=your_account_key_here
```

### CI/CD Pipeline Configuration

#### GitHub Actions
Add these secrets to your repository (Settings -> Secrets and variables -> Actions):
- `ADLS_ACCOUNT_KEY`: Your Azure storage account key

Your workflow will automatically use the ADLS configuration.


### Testing Your Configuration
1. Run your pipeline locally: `python run_polster.py --materialize --ui`
2. Check that data is written to ADLS
3. Verify assets can read from ADLS

### Troubleshooting
- **Connection failed**: Verify account name, key, and network access
- **Permission denied**: Ensure your account key has read/write permissions
- **Container not found**: Create the container in Azure Portal or grant permissions
- **Data Lake Storage Gen2**: Ensure your storage account has hierarchical namespace enabled

### Security Best Practices
- Never commit `.env` files to version control
- Use secret management in CI/CD (GitHub Secrets, Azure Key Vault, etc.)
- Rotate keys regularly
- Limit permissions to specific containers when possible

For more help, check the Polster documentation or create an issue on GitHub.
