from pathlib import Path


def test_render_blueprint_is_free_only():
    blueprint = Path("render.yaml").read_text(encoding="utf-8")

    assert "plan: free" in blueprint
    assert blueprint.count("type: web") == 1

    forbidden = (
        "plan: starter",
        "disk:",
        "futures-datahub-minio",
        "Dockerfile.minio",
        "type: worker",
        "type: cron",
        "type: keyvalue",
    )
    for token in forbidden:
        assert token not in blueprint
