import zipfile

from importlib import resources as impresources


def get_metadata(file_name: str, module: any, target_module: str):
    """
    Get full path to metadata YAML file.

    Args:
        file_name: Metadata file name (with or without .yml extension)
        module: Python module containing metadata files
        target_module: Target subdirectory within module

    Returns:
        Full path to metadata file
    """
    metadata_path = get_schemas_path(
        module=module,
        target_module=target_module
    )
    file_name = file_name if file_name.endswith('.yml') else f"{file_name}.yml"
    return f'{metadata_path}/{file_name}'


def get_schemas_path(module: any, target_module: str):
    """
    Get path to schemas directory, handling both regular and .whl packaged modules.

    Args:
        module: Python module containing schemas
        target_module: Target subdirectory within module

    Returns:
        Path to schemas directory
    """
    inputs_path = str(impresources.files(module))

    if ".whl" in str(inputs_path):
        inputs_path = get_unziped_path(inputs_path, target_module)

    return inputs_path


def get_unziped_path(input_path, target_module):  # pragma: no cover
    """
    Extract .whl file and return path to target module.

    Args:
        input_path: Path containing .whl file reference
        target_module: Target subdirectory to locate

    Returns:
        Path to extracted target module
    """
    zip_file_path = input_path.split(".whl")[0] + ".whl"
    unzip_path = "/".join(zip_file_path.split("/")[:-1]) + "/unzip/"
    unzip(zip_file_path, unzip_path)
    schemas_path = unzip_path + target_module
    return schemas_path


def unzip(source_filename, dest_dir):  # pragma: no cover
    """
    Extract zip file to destination directory.

    Args:
        source_filename: Path to zip file
        dest_dir: Destination directory for extraction
    """
    with zipfile.ZipFile(source_filename) as zf:
        zf.extractall(dest_dir)
