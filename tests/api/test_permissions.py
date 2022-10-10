from dltctl.api.permissions import PermissionsApi
import pytest
from unittest import mock

def test_get_pipeline_permissions_exists():
    client_mock = mock.MagicMock()
    api = PermissionsApi(client_mock)
    res = {"object_id": "/pipelines/12345", "access_control_list": 
    [{"user_name": "foo@example.com", "all_permissions" : [
        {
            "permission_level": "CAN_MANAGE",
            "inherited": "true"
        }
    ]}]}
    client_mock.perform_query.return_value = res
    permissions = api.get_pipeline_permissions(12345)
    
    client_mock.perform_query.assert_called_with("GET", "/permissions/pipelines/12345", data={}, headers={})
    assert permissions == res["access_control_list"]

def test_get_pipeline_permissions_no_pipeline():
    client_mock = mock.MagicMock()
    api = PermissionsApi(client_mock)
    client_mock.perform_query.side_effect = Exception("Error: RESOURCE_DOES_NOT_EXIST")
    with pytest.raises(Exception) as e:
        permissions = api.get_pipeline_permissions(12345)


def test_set_pipeline_permissions_exists():
    client_mock = mock.MagicMock()
    api = PermissionsApi(client_mock)
    acls = [{"user_name": "foo@example.com", "permission_level": "CAN_VIEW"},]
    data = {"access_control_list": acls}
    client_mock.perform_query.return_value = {"foo": "bar"}
    res = api.set_pipeline_permissions(12345, acls)
    
    client_mock.perform_query.assert_called_with("PUT", "/permissions/pipelines/12345", data=data, headers={})
    assert res["foo"] == "bar"

def test_set_pipeline_permissions_no_pipeline():
    client_mock = mock.MagicMock()
    api = PermissionsApi(client_mock)
    acls = [{"user_name": "foo@example.com", "permission_level": "CAN_VIEW"},]
    data = {"access_control_list": acls}
    client_mock.perform_query.side_effect = Exception("Error: RESOURCE_DOES_NOT_EXIST")
    with pytest.raises(Exception):
        res = api.set_pipeline_permissions(12345, acls)
    client_mock.perform_query.assert_called_with("PUT", "/permissions/pipelines/12345", data=data, headers={})


def test_get_job_permissions_exists():
    client_mock = mock.MagicMock()
    api = PermissionsApi(client_mock)
    res = {"object_id": "/jobs/12345", "access_control_list": 
    [{"user_name": "foo@example.com", "all_permissions" : [
        {
            "permission_level": "CAN_MANAGE",
            "inherited": "true"
        }
    ]}]}
    client_mock.perform_query.return_value = res
    permissions = api.get_job_permissions(12345)
    
    client_mock.perform_query.assert_called_with("GET", "/permissions/jobs/12345", data={}, headers={})
    assert permissions == res["access_control_list"]

def test_get_job_permissions_no_job():
    client_mock = mock.MagicMock()
    api = PermissionsApi(client_mock)
    client_mock.perform_query.side_effect = Exception("Error: RESOURCE_DOES_NOT_EXIST")
    with pytest.raises(Exception) as e:
        permissions = api.get_job_permissions(12345)

def test_set_job_permissions_exists():
    client_mock = mock.MagicMock()
    api = PermissionsApi(client_mock)
    acls = [{"user_name": "foo@example.com", "permission_level": "CAN_VIEW"},]
    data = {"access_control_list": acls}
    client_mock.perform_query.return_value = {"foo": "bar"}
    res = api.set_job_permissions(12345, acls)
    
    client_mock.perform_query.assert_called_with("PUT", "/permissions/jobs/12345", data=data, headers={})
    assert res["foo"] == "bar"

def test_set_job_permissions_no_exists():
    client_mock = mock.MagicMock()
    api = PermissionsApi(client_mock)
    acls = [{"user_name": "foo@example.com", "permission_level": "CAN_VIEW"},]
    data = {"access_control_list": acls}
    client_mock.perform_query.side_effect = Exception("Error: RESOURCE_DOES_NOT_EXIST")
    with pytest.raises(Exception):
        res = api.set_job_permissions(12345, acls)
    client_mock.perform_query.assert_called_with("PUT", "/permissions/jobs/12345", data=data, headers={})
