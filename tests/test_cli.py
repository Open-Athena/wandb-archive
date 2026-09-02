from wandb_archive.cli import _parser


def test_quiet_is_accepted_before_or_after_command() -> None:
    assert _parser().parse_args(["--quiet", "plan", "config.yaml"]).quiet
    assert _parser().parse_args(["plan", "config.yaml", "-q"]).quiet
