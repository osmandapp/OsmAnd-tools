package net.osmand.server.api.services.shareGpx;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FileSharedInfo {
	private Whitelist whitelist;
	private Blacklist blacklist;
	private AccessedUsers accessedUsers;

	public FileSharedInfo() {
		this(new Whitelist(), new Blacklist(), new AccessedUsers());
	}

	public FileSharedInfo(Whitelist whitelist, Blacklist blacklist, AccessedUsers accessedUsers) {
		this.whitelist = whitelist;
		this.blacklist = blacklist;
		this.accessedUsers = accessedUsers;
	}

	public Whitelist getWhitelist() {
		return whitelist;
	}

	public void setWhitelist(Whitelist whitelist) {
		this.whitelist = whitelist;
	}

	public Blacklist getBlacklist() {
		return blacklist;
	}

	public void setBlacklist(Blacklist blacklist) {
		this.blacklist = blacklist;
	}

	public AccessedUsers getAccessedUsers() {
		return accessedUsers;
	}

	public void setAccessedUsers(AccessedUsers accessedUsers) {
		this.accessedUsers = accessedUsers;
	}

	public static class Whitelist {
		private Map<Integer, Permission> permissions;

		public Whitelist() {
			this(new HashMap<>());
		}

		public Whitelist(Map<Integer, Permission> permissions) {
			this.permissions = permissions != null ? permissions : new HashMap<>();
		}

		public Map<Integer, Permission> getPermissions() {
			return permissions;
		}

		public void setPermissions(Map<Integer, Permission> permissions) {
			this.permissions = permissions;
		}
	}

	public static class Permission {
		private PermissionType permissionType;

		public Permission(PermissionType permissionType) {
			this.permissionType = permissionType;
		}

		public PermissionType getPermissionType() {
			return permissionType;
		}

		public void setPermissionType(PermissionType permissionType) {
			this.permissionType = permissionType;
		}
	}

	public enum PermissionType {
		READ,
		WRITE,
		EXECUTE
	}

	public static class Blacklist {
		private Set<Integer> users;

		public Blacklist() {
			this(new HashSet<>());
		}

		public Blacklist(Set<Integer> users) {
			this.users = users != null ? users : new HashSet<>();
		}

		public Set<Integer> getUsers() {
			return users;
		}

		public void setUsers(Set<Integer> users) {
			this.users = users;
		}
	}

	public static class AccessedUsers {
		private Set<Integer> users;

		public AccessedUsers() {
			this(new HashSet<>());
		}

		public AccessedUsers(Set<Integer> users) {
			this.users = users != null ? users : new HashSet<>();
		}

		public Set<Integer> getUsers() {
			return users;
		}

		public void setUsers(Set<Integer> users) {
			this.users = users;
		}
	}

}
