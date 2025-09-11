import json
from pathlib import Path


def fix_empty_fraction_in_json(file_path: Path):
    """Reads a JSON file, replaces "fraction": [] with "fraction": null,
    and writes the changes back to the file.

    Args:
        file_path (Path): Path to file to fix.
    """
    try:
        with file_path.open("r", encoding="utf-8") as f:
            content = f.read()

        # Simple string replacement
        new_content = content.replace('"fraction": []', '"fraction": null')

        if new_content != content:
            print(f"Found and replaced in {file_path}")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(new_content)
        else:
            print(f"No changes needed for {file_path}")

    except (json.JSONDecodeError, IOError) as e:
        print(f"Error processing {file_path}: {e}")


def main():
    """Loops through all JSON files in the specified directory and applies the fix."""
    directory = Path("data/raw/abgeordnetenwatch/votes_legislature_132/")

    if not directory.is_dir():
        print(f"Error: Directory not found at {directory}")
        return

    for file_path in directory.glob("*.json"):
        fix_empty_fraction_in_json(file_path)


if __name__ == "__main__":
    main()
