
import dataiku
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def list_files(folder, path=None, is_regex=False):
    """
    List files in a Dataiku Managed Folder.
    
    Args:
        folder (dataiku.Folder): The Dataiku Folder object.
        path (str): The path prefix to list, or a regex pattern if is_regex is True.
                    If None or empty, lists all files in the folder.
        is_regex (bool): If True, treats the 'path' argument as a regular expression pattern
                         and returns files that match the pattern.
                         If False, treats 'path' as a directory prefix.

    Returns:
        list: A list of file paths (relative to the folder root).
    """
    try:
        # Get all paths in the folder (unpartitioned assume default)
        all_paths = folder.list_paths_in_partition()
    except Exception as e:
        logger.error(f"Failed to list paths in folder: {e}")
        raise

    if not path:
        return all_paths

    if is_regex:
        try:
            pattern = re.compile(path)
            return [p for p in all_paths if pattern.search(p)]
        except re.error as e:
            logger.error(f"Invalid regex pattern '{path}': {e}")
            raise
    else:
        # Treat path as prefix
        return [p for p in all_paths if p.startswith(path)]

def copy_file(folder, src_path, dst_path):
    """
    Copy a file within the Dataiku Managed Folder.
    
    Args:
        folder (dataiku.Folder): The Dataiku Folder object.
        src_path (str): Source file path relative to folder root.
        dst_path (str): Destination file path relative to folder root.
    """
    logger.info(f"Copying file from {src_path} to {dst_path}")
    try:
        with folder.get_download_stream(src_path) as stream:
            folder.upload_stream(dst_path, stream)
        logger.info(f"Successfully copied {src_path} to {dst_path}")
    except Exception as e:
        logger.error(f"Failed to copy file from {src_path} to {dst_path}: {e}")
        raise

def delete_file(folder, path):
    """
    Delete a file from the Dataiku Managed Folder.
    
    Args:
        folder (dataiku.Folder): The Dataiku Folder object.
        path (str): File path to delete relative to folder root.
    """
    logger.info(f"Deleting file {path}")
    try:
        folder.delete_path(path)
        logger.info(f"Successfully deleted {path}")
    except Exception as e:
        logger.error(f"Failed to delete file {path}: {e}")
        raise

def move_file(folder, src_path, dst_path):
    """
    Move a file within the Dataiku Managed Folder (Copy + Delete).
    
    Args:
        folder (dataiku.Folder): The Dataiku Folder object.
        src_path (str): Source file path relative to folder root.
        dst_path (str): Destination file path relative to folder root.
    """
    logger.info(f"Moving file from {src_path} to {dst_path}")
    try:
        copy_file(folder, src_path, dst_path)
        delete_file(folder, src_path)
        logger.info(f"Successfully moved {src_path} to {dst_path}")
    except Exception as e:
        logger.error(f"Failed to move file from {src_path} to {dst_path}: {e}")
        # If copy succeeded but delete failed, we might have a duplicate. 
        # Ideally we'd handle atomic operations but S3 doesn't support them.
        raise
