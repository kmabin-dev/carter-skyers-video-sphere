import unittest
from example import config
from pathlib import Path


class TestProjectDir(unittest.TestCase):
    def test_project_dir_exists(self):
        # PROJECT_DIR may be a Path object; normalize to Path
        project_dir = config.PROJECT_DIR
        if isinstance(project_dir, str):
            project_dir = Path(project_dir)
        self.assertTrue(project_dir.exists() and project_dir.is_dir(),
                        f"PROJECT_DIR does not exist or is not a directory: {project_dir}")


if __name__ == '__main__':
    unittest.main()
