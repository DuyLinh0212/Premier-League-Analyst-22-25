"""Safety/portability regressions for launching against existing infrastructure."""
import argparse
from pathlib import Path
import socket
import tempfile
import unittest
import sys
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from infrastructure.scripts.launchers import run as launcher


class Launchers(unittest.TestCase):
    def test_env_is_literal_and_shell_overrides_container_addresses(self):
        with tempfile.TemporaryDirectory(prefix="launcher path ") as folder:
            path = Path(folder) / ".env"
            path.write_text("NEO4J_PASSWORD='p# $() secret'\nNEO4J_HTTP_URI=http://host.docker.internal:7474\n"
                            "KAFKA_BOOTSTRAP_SERVERS=remote:9092 # remote broker\n", encoding="utf-8")
            with patch.dict("os.environ", {"LOCAL_KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:29092"}, clear=True):
                env = launcher.environment(path)
            self.assertEqual(env["NEO4J_PASSWORD"], "p# $() secret")
            self.assertEqual(env["NEO4J_HTTP_URI"], "http://127.0.0.1:7474")
            self.assertEqual(env["KAFKA_BOOTSTRAP_SERVERS"], "127.0.0.1:29092")
            self.assertIn(str(launcher.ROOT), env["PYTHONPATH"])

    def test_missing_container_prevents_partial_start(self):
        with patch("infrastructure.scripts.launchers.run.shutil.which", return_value="docker"), patch("infrastructure.scripts.launchers.run.checked") as docker:
            docker.side_effect = ["version", '{"Status":"exited"}', RuntimeError("not found")]
            with self.assertRaisesRegex(RuntimeError, "Existing container not found: missing"):
                launcher.infrastructure("airflow", {"AIRFLOW_CONTAINERS": "existing,missing"})
            self.assertTrue(all(call.args[0][1] in ("info", "inspect") for call in docker.call_args_list))

    def test_container_names_cannot_be_cli_options(self):
        with self.assertRaises(RuntimeError):
            launcher.names("kafka", {"KAFKA_CONTAINER": "--help"})

    def test_check_does_not_start_any_resources(self):
        args = argparse.Namespace(service="kafka", env_file=Path(".env"), check=True)
        with patch("infrastructure.scripts.launchers.run.environment", return_value={}), patch("infrastructure.scripts.launchers.run.infrastructure", return_value=[("broker", {"Status": "exited"})]), patch("infrastructure.scripts.launchers.run.start_infrastructure") as start, patch("infrastructure.scripts.launchers.run.subprocess.Popen") as process:
            self.assertEqual(launcher.run(args), 0)
            start.assert_not_called()
            process.assert_not_called()

    def test_occupied_api_port_is_rejected(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            args = argparse.Namespace(api_port=listener.getsockname()[1])
            env = {"NEO4J_PASSWORD": "test", "NEO4J_HTTP_URI": "http://127.0.0.1:7474", "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:29092"}
            with patch("infrastructure.scripts.launchers.run.importlib.util.find_spec", return_value=object()):
                with self.assertRaisesRegex(RuntimeError, "Cannot bind backend"):
                    launcher.app_command("backend", env, args)

    def test_all_cleans_up_owned_app_when_second_spawn_fails(self):
        args = argparse.Namespace(service="all", env_file=Path(".env"), check=False)
        apps = [(["python", "backend"], launcher.ROOT, "127.0.0.1", 8000), (["node", "frontend"], launcher.WEB, "127.0.0.1", 3000)]
        owned = object()
        with patch("infrastructure.scripts.launchers.run.environment", return_value={}), patch("infrastructure.scripts.launchers.run.infrastructure", return_value=[]), patch("infrastructure.scripts.launchers.run.app_command", side_effect=apps), patch("infrastructure.scripts.launchers.run.subprocess.Popen", side_effect=[owned, OSError("spawn failed")]), patch("infrastructure.scripts.launchers.run.stop_apps") as stop:
            with self.assertRaises(OSError):
                launcher.run(args)
            stop.assert_called_once_with([owned])


if __name__ == "__main__":
    unittest.main()
