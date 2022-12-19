from ..clusterview import ClusterView
import pytest

@pytest.fixture(scope="session")
def cv():
    clv = ClusterView()
    return clv

def test_instantiate(cv):
    assert cv is not None

# For testing settings types
@pytest.mark.parametrize("setting, obj_type", [
    ("frane_button","DoSomething"),
    ("choose_run","Choice"),
    ("update_button","DoSomething"),
    ("settings_button","DoSomething"),
    ("logout_button","DoSomething"),
    ("run_folder_name","Text"),
    ("dest_folder","Directory"),
    ("download_button","DoSomething"),
    ])

def test_create_settings(cv,setting,obj_type):
    # Test all settings are expected type.
    cv.settings()
    cv_props = vars(cv)
    assert type(cv_props.get(setting)).__name__ == obj_type

# Other tests pending on light refactoring of on_download_click