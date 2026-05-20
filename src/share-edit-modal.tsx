import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { ReactWidget } from '@jupyterlab/ui-components';
import { Widget } from '@lumino/widgets';
import {
  ShareGranteeDetail,
  UserSearchResult,
  GroupSearchResult,
  fetchSharesForResource,
  removeShare,
  createShare,
  findUsers,
  findGroups
} from './shares';
import { createDebouncedFetcher } from './debounce';
import { TrashIcon, UserEmoji, GroupEmoji } from './icons';

export interface ShareTarget {
  name: string;
  rawPath: string;
}

function GranteeItem({
  grantee,
  onRemove
}: {
  grantee: ShareGranteeDetail;
  onRemove: (shareId: string) => Promise<void>;
}) {
  return (
    <div className="swan-shares-grantee-item">
      <div className="swan-shares-grantee-info">
        <span className="swan-shares-grantee-icon" title={grantee.type === 'GRANTEE_TYPE_USER' ? 'User' : 'Group'}>
          {grantee.type === 'GRANTEE_TYPE_USER' ? UserEmoji : GroupEmoji}
        </span>
        <span className="swan-shares-grantee-name">{grantee.opaqueId}</span>
      </div>
      <button className="swan-shares-grantee-remove" title="Remove" onClick={() => onRemove(grantee.shareId)}>
        {TrashIcon}
      </button>
    </div>
  );
}

function SearchResultItem({
  icon,
  name,
  detail,
  alreadyAdded,
  onAdd
}: {
  icon: string;
  iconTitle: string;
  name: string;
  detail?: string;
  alreadyAdded: boolean;
  onAdd: () => Promise<void>;
}) {
  const [state, setState] = useState<'idle' | 'adding' | 'added'>(alreadyAdded ? 'added' : 'idle');

  useEffect(() => {
    setState(prev => (prev === 'adding' ? prev : alreadyAdded ? 'added' : 'idle'));
  }, [alreadyAdded]);

  const handleAdd = async () => {
    setState('adding');
    try {
      await onAdd();
      setState('added');
    } catch {
      setState('idle');
    }
  };

  return (
    <div className="swan-shares-search-result-item">
      <span className="swan-shares-grantee-icon">{icon}</span>
      <div className="swan-shares-search-result-info">
        <div className="swan-shares-search-result-name">{name}</div>
        {detail && <div className="swan-shares-search-result-detail">{detail}</div>}
      </div>
      <button className="swan-shares-search-result-add" disabled={state !== 'idle'} onClick={handleAdd}>
        {state === 'adding' ? 'Adding...' : state === 'added' ? 'Added' : 'Add'}
      </button>
    </div>
  );
}

function EditShareModalContent({ share, onClose }: { share: ShareTarget; onClose: () => void }) {
  const [grantees, setGrantees] = useState<ShareGranteeDetail[]>([]);
  const [granteesLoading, setGranteesLoading] = useState(true);
  const [granteesError, setGranteesError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<{ users: UserSearchResult[]; groups: GroupSearchResult[] } | null>(
    null
  );
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [status, setStatus] = useState<{ msg: string; isError: boolean } | null>(null);
  const fetcher = useMemo(() => createDebouncedFetcher(300), []);

  const showStatus = useCallback((msg: string, isError = false) => {
    setStatus({ msg, isError });
    if (!isError) {
      setTimeout(() => setStatus(prev => (prev?.msg === msg ? null : prev)), 3000);
    }
  }, []);

  const loadGrantees = useCallback(
    async (silent = false) => {
      if (!silent) {
        setGranteesLoading(true);
        setGranteesError(null);
      }
      try {
        const data = await fetchSharesForResource(share.rawPath);
        setGrantees(data);
      } catch (err) {
        if (!silent) {
          setGranteesError(err instanceof Error ? err.message : 'Failed to load');
        }
      } finally {
        if (!silent) {
          setGranteesLoading(false);
        }
      }
    },
    [share.rawPath]
  );

  useEffect(() => {
    loadGrantees();
  }, [loadGrantees]);

  useEffect(() => {
    const query = searchQuery.trim();
    if (query.length < 2) {
      setSearchResults(null);
      setSearchLoading(false);
      setSearchError(null);
      return fetcher.cancel;
    }
    fetcher.run(async signal => {
      setSearchLoading(true);
      try {
        const [users, groups] = await Promise.all([findUsers(query, signal), findGroups(query, signal)]);
        setSearchResults({ users, groups });
        setSearchError(null);
      } catch (err) {
        if (signal.aborted) return;
        setSearchError(err instanceof Error ? err.message : 'Search failed');
        setSearchResults(null);
      } finally {
        if (!signal.aborted) setSearchLoading(false);
      }
    });
    return fetcher.cancel;
  }, [searchQuery, fetcher]);

  const handleRemove = async (shareId: string) => {
    try {
      await removeShare(shareId);
      setGrantees(prev => prev.filter(g => g.shareId !== shareId));
    } catch (err) {
      showStatus(`Failed to remove: ${err instanceof Error ? err.message : 'Unknown error'}`, true);
    }
  };

  // We only support read-only (VIEWER) shares for now.
  const handleAddUser = async (user: UserSearchResult) => {
    try {
      await createShare(share.rawPath, user.opaqueId, user.idp, 'VIEWER', 'GRANTEE_TYPE_USER');
      setGrantees(prev => [
        ...prev,
        { shareId: `pending-${user.opaqueId}`, type: 'GRANTEE_TYPE_USER', opaqueId: user.opaqueId, role: 'VIEWER' }
      ]);
      loadGrantees(true);
    } catch (err) {
      showStatus(`Failed to add: ${err instanceof Error ? err.message : 'Unknown error'}`, true);
      throw err;
    }
  };

  const handleAddGroup = async (group: GroupSearchResult) => {
    try {
      await createShare(share.rawPath, group.opaqueId, '', 'VIEWER', 'GRANTEE_TYPE_GROUP');
      setGrantees(prev => [
        ...prev,
        { shareId: `pending-${group.opaqueId}`, type: 'GRANTEE_TYPE_GROUP', opaqueId: group.opaqueId, role: 'VIEWER' }
      ]);
      loadGrantees(true);
    } catch (err) {
      showStatus(`Failed to add: ${err instanceof Error ? err.message : 'Unknown error'}`, true);
      throw err;
    }
  };

  return (
    <div
      className="swan-shares-modal-overlay"
      onMouseDown={e => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="swan-shares-modal">
        <div className="swan-shares-modal-header">
          <h3 className="swan-shares-modal-title">Share: {share.name}</h3>
          <button className="swan-shares-modal-close-btn" onClick={onClose}>
            {'\u00d7'}
          </button>
        </div>

        <div className="swan-shares-modal-body">
          {/* Search section */}
          <div className="swan-shares-modal-section">
            <div className="swan-shares-modal-section-title">Share with people</div>
            <div className="swan-shares-search-container">
              <input
                className="swan-shares-search-input"
                type="text"
                placeholder="Search users or groups..."
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
              />
              <div className="swan-shares-search-results">
                {searchLoading && <div className="swan-shares-modal-empty">Searching...</div>}
                {searchError && (
                  <div className="swan-shares-modal-empty" style={{ color: 'var(--jp-error-color1)' }}>
                    {searchError}
                  </div>
                )}
                {searchResults &&
                  !searchLoading &&
                  searchResults.users.length === 0 &&
                  searchResults.groups.length === 0 && <div className="swan-shares-modal-empty">No results found</div>}
                {searchResults && !searchLoading && (
                  <>
                    {searchResults.users.map(user => (
                      <SearchResultItem
                        key={`user-${user.opaqueId}`}
                        icon={UserEmoji}
                        iconTitle="User"
                        name={user.displayName || user.opaqueId}
                        detail={user.mail || undefined}
                        alreadyAdded={grantees.some(
                          g => g.type === 'GRANTEE_TYPE_USER' && g.opaqueId === user.opaqueId
                        )}
                        onAdd={() => handleAddUser(user)}
                      />
                    ))}
                    {searchResults.groups.map(group => (
                      <SearchResultItem
                        key={`group-${group.opaqueId}`}
                        icon={GroupEmoji}
                        iconTitle="Group"
                        name={group.displayName}
                        alreadyAdded={grantees.some(
                          g => g.type === 'GRANTEE_TYPE_GROUP' && g.opaqueId === group.opaqueId
                        )}
                        onAdd={() => handleAddGroup(group)}
                      />
                    ))}
                  </>
                )}
              </div>
            </div>
          </div>

          {/* Grantees section - hidden when empty */}
          {!granteesLoading && !granteesError && grantees.length > 0 && (
            <div className="swan-shares-modal-section">
              <div className="swan-shares-modal-section-title">Shared with</div>
              <div className="swan-shares-grantee-list">
                {grantees.map(g => (
                  <GranteeItem key={g.shareId} grantee={g} onRemove={handleRemove} />
                ))}
              </div>
            </div>
          )}
          {granteesLoading && <div className="swan-shares-modal-empty">Loading...</div>}
          {granteesError && (
            <div className="swan-shares-modal-empty" style={{ color: 'var(--jp-error-color1)' }}>
              {granteesError}
            </div>
          )}

          {status && (
            <div
              className="swan-shares-modal-status"
              style={{ color: status.isError ? 'var(--jp-error-color1)' : 'var(--jp-ui-font-color2)' }}
            >
              {status.msg}
            </div>
          )}
        </div>

        <div className="swan-shares-modal-footer">
          <button className="swan-shares-modal-footer-btn" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

let activeModal: Widget | null = null;

export function openEditShareModal(share: ShareTarget, onClose: () => void): void {
  if (activeModal) {
    activeModal.dispose();
    activeModal = null;
  }

  const widget = ReactWidget.create(
    <EditShareModalContent
      share={share}
      onClose={() => {
        widget.dispose();
        activeModal = null;
        onClose();
      }}
    />
  );
  activeModal = widget;
  Widget.attach(widget, document.body);
}
