"""
Environment configuration for multi-environment deployments.
"""
from typing import Dict, Any
from aws_cdk import Environment as CdkEnvironment


class EnvironmentConfig:
    """Configuration for different deployment environments."""
    
    def __init__(self, name: str, account: str = None, region: str = None, **kwargs):
        """
        Initialize environment configuration.
        
        Args:
            name: Environment name (e.g., 'dev', 'staging', 'prod')
            account: AWS account ID (optional, uses default if not provided)
            region: AWS region (optional, uses default if not provided)
            **kwargs: Additional environment-specific configuration
        """
        self.name = name
        self.account = account
        self.region = region
        self.config = kwargs
    
    def get_cdk_environment(self) -> CdkEnvironment:
        """Get CDK Environment object for this configuration."""
        return CdkEnvironment(
            account=self.account,
            region=self.region
        )
    
    def get_resource_name(self, base_name: str) -> str:
        """Generate environment-specific resource name."""
        env_suffix = self.name.lower()
        return f"{base_name}-{env_suffix}"
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get environment-specific configuration value."""
        return self.config.get(key, default)


# Default environment configurations
ENVIRONMENTS: Dict[str, EnvironmentConfig] = {
    "dev": EnvironmentConfig(
        name="dev",
        # account and region can be set via CDK context or environment variables
    ),
    "sandbox": EnvironmentConfig(
        name="sandbox",
    ),
    "sandox": EnvironmentConfig(
        name="sandox",
    ),
    "uat": EnvironmentConfig(
        name="uat",
    ),
    "qe": EnvironmentConfig(
        name="qe",
    ),
    "staging": EnvironmentConfig(
        name="staging",
    ),
    "prod": EnvironmentConfig(
        name="prod",
    ),
}


def get_environment(env_name: str = None) -> EnvironmentConfig:
    """
    Get environment configuration by name.
    
    Args:
        env_name: Environment name. If None, tries to get from CDK context or defaults to 'dev'
    
    Returns:
        EnvironmentConfig instance
    """
    if env_name is None:
        env_name = "dev"  # Default to dev
    
    env_name = env_name.lower()
    
    if env_name not in ENVIRONMENTS:
        raise ValueError(
            f"Unknown environment: {env_name}. "
            f"Available environments: {', '.join(ENVIRONMENTS.keys())}"
        )
    
    return ENVIRONMENTS[env_name]

