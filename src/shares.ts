import { ServerConnection } from '@jupyterlab/services';

// TODO: Are there other grantee types?
type GranteeType = 'GRANTEE_TYPE_USER' | 'GRANTEE_TYPE_GROUP';
type ShareDirection = 'BY_ME' | 'WITH_ME';
type ShareType = 'REGULAR' | 'PUBLIC';
// TODO: What other share states are there?
type ShareState = 'SHARE_STATE_ACCEPTED';

interface RawUserGrantee {
  type: 'GRANTEE_TYPE_USER';
  user_id: { opaque_id: string };
}

interface RawGroupGrantee {
  type: 'GRANTEE_TYPE_GROUP';
  group_id: { opaque_id: string };
}

interface RawResourceInfo {
  name: string;
  path: string;
}

interface RawSharedByMeRegular {
  share: {
    id: { opaque_id: string };
    resource_id: { opaque_id: string };
    grantee: RawUserGrantee | RawGroupGrantee;
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
      grantee: RawUserGrantee | RawGroupGrantee;
      creator: { opaque_id: string };
    };
  };
  resource_info: RawResourceInfo;
}

interface Grantee {
  type: GranteeType;
  opaqueId: string;
}

interface _Share {
  shareDirection: ShareDirection;
  shareType: ShareType;
  resourceOpaueId: string;
  name: string;
  path: string;
}

interface ByMeRegularShare extends _Share {
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
}

export type Share = ByMeRegularShare | ByMePublicShare | WithMeRegularShare;

export async function fetchShares(): Promise<Share[]> {
  const settings = ServerConnection.makeSettings();

  const [byMeResp, withMeResp] = await Promise.all([
    ServerConnection.makeRequest(settings.baseUrl + 'share/getSharedByMe', {}, settings),
    ServerConnection.makeRequest(settings.baseUrl + 'share/getSharedWithMe', {}, settings)
  ]);

  if (!byMeResp.ok) {
    const data = await byMeResp.json();
    throw new ServerConnection.ResponseError(byMeResp, data.error ?? byMeResp.statusText);
  }
  if (!withMeResp.ok) {
    const data = await withMeResp.json();
    throw new ServerConnection.ResponseError(withMeResp, data.error ?? withMeResp.statusText);
  }

  const byMeData = await byMeResp.json();
  const withMeData = await withMeResp.json();
  console.log('[cs3org/cs3-jupyter-client] Shares shared by me:', byMeData);
  console.log('[cs3org/cs3-jupyter-client] Shares shared with me:', withMeData);

  const byMeMerged = new Map<string, ByMeRegularShare>();
  for (const share of byMeData.shares as RawSharedByMeRegular[]) {
    const resourceId = share.share.resource_id.opaque_id;
    const sharedWithOpaqueId =
      share.share.grantee.type === 'GRANTEE_TYPE_USER'
        ? share.share.grantee.user_id.opaque_id
        : share.share.grantee.group_id.opaque_id;

    if (!byMeMerged.has(resourceId)) {
      byMeMerged.set(resourceId, {
        shareDirection: 'BY_ME',
        shareType: 'REGULAR',
        resourceOpaueId: share.share.resource_id.opaque_id,
        name: share.resource_info.name,
        path: share.resource_info.path.slice('/eos'.length), // TODO: Don't hardcode this prefix
        sharedWith: [
          {
            type: share.share.grantee.type,
            opaqueId: sharedWithOpaqueId
          }
        ]
      });
    } else {
      byMeMerged.get(resourceId)?.sharedWith.push({
        type: share.share.grantee.type,
        opaqueId: sharedWithOpaqueId
      });
    }
  }

  const byMePublicMerged = new Map<string, ByMePublicShare>();
  for (const share of byMeData.public_shares as RawSharedByMePublic[]) {
    const resourceId = share.public_share.resource_id.opaque_id;
    byMePublicMerged.set(resourceId, {
      shareDirection: 'BY_ME',
      shareType: 'PUBLIC',
      resourceOpaueId: share.public_share.resource_id.opaque_id,
      name: share.resource_info.name,
      path: share.resource_info.path.slice('/eos'.length) // TODO: Don't hardcode this prefix
    });
  }

  const withMeMerged = new Map<string, WithMeRegularShare>();
  for (const share of withMeData.shares as RawSharedWithMe[]) {
    if (share.received_share.state !== 'SHARE_STATE_ACCEPTED') {
      continue; // Skip non-accepted shares
    }
    const resourceId = share.received_share.share.resource_id.opaque_id;
    withMeMerged.set(resourceId, {
      shareDirection: 'WITH_ME',
      shareType: 'REGULAR',
      resourceOpaueId: share.received_share.share.resource_id.opaque_id,
      name: share.resource_info.name,
      path: share.resource_info.path.slice('/eos'.length), // TODO: Don't hardcode this prefix
      sharedBy: share.received_share.share.creator.opaque_id
    });
  }

  return [...byMeMerged.values(), ...byMePublicMerged.values(), ...withMeMerged.values()];
}
