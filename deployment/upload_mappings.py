import os
import subprocess


def upload_mappings(folder_path):
    if not os.path.exists(folder_path):
        raise ValueError(f"The folder {folder_path} does not exist.")

    for version_folder in os.listdir(folder_path):
        version_folder_path = os.path.join(folder_path, version_folder)

        if not os.path.isdir(version_folder_path):
            continue

        print(f"Processing version: {version_folder}")

        for type_folder in os.listdir(version_folder_path):
            type_folder_path = os.path.join(version_folder_path, type_folder)

            if not os.path.isdir(type_folder_path):
                continue

            json_files_path = os.path.join(type_folder_path, "*.json")

            try:
                subprocess.run(
                    [
                        "snow",
                        "--config-file",
                        "./iaa_config.toml",
                        "stage",
                        "copy",
                        json_files_path,
                        f"@SMA_MAPPINGS/{version_folder}/{type_folder}/",
                        "--overwrite",
                        "-c",
                        "iaa",
                    ],
                    check=True,
                )
                print(f"Successfully uploaded files for {version_folder}/{type_folder}")
            except subprocess.CalledProcessError as e:
                print(f"Error uploading files for {version_folder}/{type_folder}: {e}")


upload_mappings("../maps")