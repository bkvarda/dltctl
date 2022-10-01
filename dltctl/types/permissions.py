from dltctl.types.pipelines import AccessConfig

class AclList:
    def __init__(self):
        self.acls = None
        self.owner = None
        self.owner_type = None
        self.owner_permissions = []
        self.group_permissions = []
        self.user_permissions = []
        self.sp_permissions = []
        self.user_managers = []
        self.user_viewers = []
        self.group_managers = []
        self.group_viewers = []
        self.sp_managers = []
        self.sp_viewers = []

    def _sort_permission(self, principal_type, principal, perm):
        group_managers = self.group_managers
        group_viewers = self.group_viewers
        user_managers = self.user_managers
        user_viewers = self.user_viewers
        sp_managers = self.sp_managers
        sp_viewers = self.sp_viewers
        owner_permissions =  self.owner_permissions
        permission_level = perm["permission_level"]
        if permission_level == 'IS_OWNER':
            self.owner = principal
            self.owner_type = principal_type
            owner_permissions.append({principal_type: principal, "permission_level": 'IS_OWNER'})
            self.owner_permissions = owner_permissions
        elif permission_level == 'CAN_MANAGE':
            if principal_type == 'group_name':
                group_managers.append(principal)
                self.group_managers = group_managers
            elif principal_type == 'user_name':
                user_managers.append(principal)
                self.user_managers = user_managers
            else:
                sp_managers.append(principal)
                self.sp_managers = sp_managers
        elif permission_level == 'CAN_VIEW':
            if principal_type == 'group_name':
                group_viewers.append(principal)
                self.group_viewers = group_viewers
            elif principal_type == 'user_name':
                user_viewers.append(principal)
                self.user_viewers = user_viewers
            else:
                sp_viewers.append(principal)
                self.sp_viewers = sp_viewers
        return

    def add(self, principal_type, principal, permission_level):
        if principal_type not in ['user_name','group_name','service_principal_name']:
            raise ValueError(f"Invalid principal type {principal_type}. Should be user_name, group_name or service_principal_name")
        
        if permission_level not in ['CAN_VIEW','CAN_MANAGE','IS_OWNER']:
            raise ValueError(f"Invalid permission_level {permission_level}. Only CAN_VIEW, CAN_MANAGE, IS_OWNER supported")

        acl = {principal_type: principal, "permission_level": permission_level}
        self._sort_permission(principal_type, principal, acl)


    def from_arr(self, acls):
        user_permissions = []
        group_permissions = []
        sp_permissions = []
        self_bck = self
        try:
            for acl in acls:
                if "user_name" in acl:
                    user_permissions.append(acl)
                    for perm in acl["all_permissions"]:
                        self._sort_permission("user_name",acl["user_name"],perm)
                elif "group_name" in acl:
                    group_permissions.append(acl)
                    for perm in acl["all_permissions"]:
                        self._sort_permission("group_name",acl["group_name"],perm)
                elif "service_principal_name" in acl:
                    sp_permissions.append(acl)
                    for perm in acl["all_permissions"]:
                        self._sort_permission("service_principal_name",acl["service_principal_name"],perm)
                else:
                    raise ValueError(f"Invalid acls - acls contain invalid acl: {acl}")
        except:
            # Reset state
            self=self_bck
            raise

        self.user_permissions = user_permissions
        self.group_permissions = group_permissions
        self.sp_permissions = sp_permissions
        return self

    def to_arr(self):
        return self.owner_permissions + self.group_managers + self.group_viewers

    def to_access_config(self):
        return AccessConfig(
            manager_groups=self.group_managers if len(self.group_managers) > 0 else None,
            reader_groups=self.group_viewers if len(self.group_viewers) > 0 else None,
            notification_group=None
        )
    def from_access_config(self, access_config):
        mgrs = access_config.manager_groups
        readers = access_config.reader_groups
        group_managers = []
        group_viewers = []
        if mgrs:
            for group in mgrs:
                group_managers.append({"group_name": group, "permission_level": 'CAN_MANAGE'})
        if readers:
            for group in readers:
                group_viewers.append({"group_name": group, "permission_level": 'CAN_VIEW'})
        self.group_viewers = group_viewers
        self.group_managers = group_managers
        return self