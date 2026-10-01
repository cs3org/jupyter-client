import { requestAPI } from './api';
import { toApiPath } from './paths';

type GranteeType = 'GRANTEE_TYPE_USER' | 'GRANTEE_TYPE_GROUP' | 'GRANTEE_TYPE_INVALID';
type ShareDirection = 'BY_ME' | 'WITH_ME';
type ShareType = 'REGULAR' | 'PUBLIC';
type ShareState = 'SHARE_STATE_ACCEPTED' | 'SHARE_STATE_PENDING' | 'SHARE_STATE_REJECTED';
export type ShareRole = 'VIEWER' | 'EDITOR';

interface RawUserGrantee {
  type: 'GRANTEE_TYPE_USER';
  user_id: { opaque_id: string };
}

interface RawGroupGrantee {
  type: 'GRANTEE_TYPE_GROUP';
  group_id: { opaque_id: string };
}

type RawGrantee = RawUserGrantee | RawGroupGrantee;

export type ResourceType = 'RESOURCE_TYPE_CONTAINER' | 'RESOURCE_TYPE_FILE';

interface RawResourceInfo {
  name: string;
  path: string;
  type?: ResourceType;
}

interface RawSharePermissions {
  permissions?: { initiate_file_upload?: boolean };
}

interface RawSharedByMeRegular {
  share: {
    id: { opaque_id: string };
    resource_id: { opaque_id: string };
    grantee: RawGrantee;
    permissions?: RawSharePermissions;
  };
  resource_info: RawResourceInfo;
}

interface RawSharedByMePublic {
  public_share: {
    id: { opaque_id: string };
    resource_id: { opaque_id: string };
  };
  resource_info: RawResourceInfo;
}

interface RawSharedWithMe {
  received_share: {
    state: ShareState | string;
    share: {
      id: { opaque_id: string };
      resource_id: { opaque_id: string };
      grantee: RawGrantee;
      creator: { opaque_id: string };
      permissions?: RawSharePermissions;
    };
  };
  resource_info: RawResourceInfo;
}

interface RawUser {
  id: { opaque_id: string; idp: string };
  display_name?: string;
  username?: string;
  mail?: string;
}

interface RawGroup {
  id: { opaque_id: string };
  group_name?: string;
}

interface Grantee {
  type: GranteeType;
  opaqueId: string;
}

interface _Share {
  shareDirection: ShareDirection;
  shareType: ShareType;
  resourceOpaqueId: string;
  resourceType: ResourceType;
  name: string;
  path: string;
  rawPath: string;
}

export interface ByMeRegularShare extends _Share {
  shareDirection: 'BY_ME';
  shareType: 'REGULAR';
  sharedWith: Grantee[];
}

interface ByMePublicShare extends _Share {
  shareDirection: 'BY_ME';
  shareType: 'PUBLIC';
}

interface WithMeRegularShare extends _Share {
  shareDirection: 'WITH_ME';
  shareType: 'REGULAR';
  sharedBy: string;
  role: ShareRole;
}

export type Share = ByMeRegularShare | ByMePublicShare | WithMeRegularShare;

function granteeOpaqueId(grantee: RawGrantee): string {
  return grantee.type === 'GRANTEE_TYPE_USER' ? grantee.user_id.opaque_id : grantee.group_id.opaque_id;
}

function roleFromPermissions(permissions: RawSharePermissions | undefined): ShareRole {
  return permissions?.permissions?.initiate_file_upload ? 'EDITOR' : 'VIEWER';
}

/** The fields every kind of share takes from the resource it shares. */
function resourceFields(resourceId: string, info: RawResourceInfo): Omit<_Share, 'shareDirection' | 'shareType'> {
  return {
    resourceOpaqueId: resourceId,
    resourceType: info.type ?? 'RESOURCE_TYPE_CONTAINER',
    name: info.name,
    path: toApiPath(info.path),
    rawPath: info.path
  };
}

export async function fetchShares(): Promise<Share[]> {
  const [byMe, withMe] = await Promise.all([
    requestAPI<{ shares: RawSharedByMeRegular[]; public_shares: RawSharedByMePublic[] }>('share/getSharedByMe'),
    requestAPI<{ shares: RawSharedWithMe[] }>('share/getSharedWithMe')
  ]);

  // One entry per shared resource, listing everyone it is shared with
  const byMeRegular = new Map<string, ByMeRegularShare>();
  for (const { share, resource_info } of byMe.shares) {
    const resourceId = share.resource_id.opaque_id;
    const grantee = { type: share.grantee.type, opaqueId: granteeOpaqueId(share.grantee) };
    const existing = byMeRegular.get(resourceId);
    if (existing) {
      existing.sharedWith.push(grantee);
    } else {
      byMeRegular.set(resourceId, {
        shareDirection: 'BY_ME',
        shareType: 'REGULAR',
        ...resourceFields(resourceId, resource_info),
        sharedWith: [grantee]
      });
    }
  }

  const byMePublic = new Map<string, ByMePublicShare>();
  for (const { public_share, resource_info } of byMe.public_shares) {
    const resourceId = public_share.resource_id.opaque_id;
    byMePublic.set(resourceId, {
      shareDirection: 'BY_ME',
      shareType: 'PUBLIC',
      ...resourceFields(resourceId, resource_info)
    });
  }

  const withMeAccepted = new Map<string, WithMeRegularShare>();
  for (const { received_share, resource_info } of withMe.shares) {
    if (received_share.state !== 'SHARE_STATE_ACCEPTED') {
      continue; // Skip non-accepted shares
    }
    const { share } = received_share;
    const resourceId = share.resource_id.opaque_id;
    withMeAccepted.set(resourceId, {
      shareDirection: 'WITH_ME',
      shareType: 'REGULAR',
      ...resourceFields(resourceId, resource_info),
      sharedBy: share.creator.opaque_id,
      role: roleFromPermissions(share.permissions)
    });
  }

  // Keep only read-only shares
  const readOnlyWithMe = [...withMeAccepted.values()].filter(s => s.role === 'VIEWER');

  return [...byMeRegular.values(), ...byMePublic.values(), ...readOnlyWithMe];
}

export interface ShareGranteeDetail {
  shareId: string;
  type: GranteeType;
  opaqueId: string;
  role: ShareRole;
}

export interface UserSearchResult {
  opaqueId: string;
  idp: string;
  displayName: string;
  mail: string;
}

export interface GroupSearchResult {
  opaqueId: string;
  displayName: string;
}

export async function fetchSharesForResource(rawPath: string): Promise<ShareGranteeDetail[]> {
  const data = await requestAPI<{ shares: RawSharedByMeRegular[] }>('share/getSharedByResource', {}, { path: rawPath });
  return data.shares.map(({ share }) => ({
    shareId: share.id.opaque_id,
    type: share.grantee.type,
    opaqueId: granteeOpaqueId(share.grantee),
    role: roleFromPermissions(share.permissions)
  }));
}

export async function removeShare(shareId: string): Promise<void> {
  await requestAPI('share/share', { method: 'DELETE' }, { share_id: shareId });
}

export async function updateShareRole(shareId: string, role: ShareRole): Promise<void> {
  await requestAPI('share/share', { method: 'PUT', body: JSON.stringify({ role }) }, { share_id: shareId });
}

export async function createShare(
  rawPath: string,
  opaqueId: string,
  idp: string,
  role: ShareRole,
  granteeType: GranteeType
): Promise<void> {
  const body = JSON.stringify({
    opaque_id: opaqueId,
    idp,
    role,
    grantee_type: granteeType === 'GRANTEE_TYPE_USER' ? 'USER' : 'GROUP'
  });
  await requestAPI('share/share', { method: 'POST', body }, { path: rawPath });
}

export async function findUsers(query: string, signal?: AbortSignal): Promise<UserSearchResult[]> {
  const data = await requestAPI<{ items: RawUser[] }>('find/users', { signal }, { search: query });
  return data.items.map(u => ({
    opaqueId: u.id.opaque_id,
    idp: u.id.idp,
    displayName: u.display_name || u.username || '',
    mail: u.mail || ''
  }));
}

export async function findGroups(query: string, signal?: AbortSignal): Promise<GroupSearchResult[]> {
  const data = await requestAPI<{ items: RawGroup[] }>('find/groups', { signal }, { search: query });
  return data.items.map(g => ({
    opaqueId: g.id.opaque_id,
    displayName: g.group_name || g.id.opaque_id
  }));
}
