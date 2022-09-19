import pytest
from unittest import mock
from dltctl.types.permissions import AclList


_TEST_ACLS = [{'group_name': 'admins', 'all_permissions': [{'permission_level': 'CAN_MANAGE', 'inherited': True, 'inherited_from_object': ['/pipelines/']}]}, 
{'user_name': 'brandon@foo.com', 'all_permissions': [{'permission_level': 'IS_OWNER', 'inherited': False}]}, 
{'group_name': 'analysts', 'all_permissions': [{'permission_level': 'CAN_VIEW', 'inherited': False}]},
{'group_name': 'analysts', 'all_permissions': [{'permission_level': 'CAN_MANAGE', 'inherited': False}]}]

_BAD_TEST_ACLS = [{'group_name': 'admins', 'all_permissions': [{'permission_level': 'CAN_MANAGE', 'inherited': True, 'inherited_from_object': ['/pipelines/']}]}, 
{'group_': 'failgroup', 'all_permissions': [{'permission_level': 'CAN_MG', 'inherited': False}]},
{'user_name': 'brandon@foo.com', 'all_permissions': [{'permission_level': 'IS_OWNER', 'inherited': False}]}, 
{'group_name': 'analysts', 'all_permissions': [{'permission_level': 'CAN_VIEW', 'inherited': False}]}]


def test_acls_from_api_res():
    acl_list = AclList().from_arr(_TEST_ACLS)
    assert acl_list.owner == 'brandon@foo.com'
    assert acl_list.owner_type == 'user_name'
    assert 'analysts' in acl_list.group_managers
    assert 'admins' in acl_list.group_managers
    assert 'analysts' in acl_list.group_viewers
    assert len(acl_list.user_viewers) == 0
    assert len(acl_list.user_managers) == 0

def test_acls_add():
    acl_list = AclList()
    acl_list.add("user_name", "foo@foo.com", 'CAN_VIEW')
    acl_list.add("group_name", "foo", 'CAN_MANAGE')
    acl_list.add("service_principal_name", "123456", 'CAN_VIEW')
    acl_list.add("service_principal_name", "123456", 'CAN_MANAGE')
    acl_list.add("user_name", "owner@baz.com", 'IS_OWNER')
    assert 'foo@foo.com' in acl_list.user_viewers
    assert 'foo' in acl_list.group_managers
    assert '123456' in acl_list.sp_viewers
    assert '123456' in acl_list.sp_managers
    assert 'owner@baz.com' == acl_list.owner
    assert 'user_name' == acl_list.owner_type

def test_invalid_acl_add_type():
    acl_list = AclList()
    with pytest.raises(ValueError):
        acl_list.add('foo_type','foo','IS_OWNER')

def test_invalid_acl_add_acl():
    acl_list = AclList()
    with pytest.raises(ValueError):
        acl_list.add('foo_type','foo','IS_OWNEROO')

def test_failed_from_arr_reset():
    acl_list = AclList().from_arr(_TEST_ACLS)
    try:
        acl_list.from_arr(_BAD_TEST_ACLS)
    except:
        #do nothing
        pass

    assert acl_list.owner == 'brandon@foo.com'
    assert acl_list.owner_type == 'user_name'
    assert 'analysts' in acl_list.group_managers
    assert 'admins' in acl_list.group_managers
    assert 'analysts' in acl_list.group_viewers
    assert len(acl_list.user_viewers) == 0
    assert len(acl_list.user_managers) == 0

    
