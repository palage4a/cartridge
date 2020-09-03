// @flow
import * as React from 'react';
import { css, cx } from 'react-emotion';
import {
  Button,
  Dropdown,
  DropdownItem,
  IconMore
} from '@tarantool.io/ui-kit';
import store from 'src/store/instance';
import { failoverPromoteLeader, showExpelModal } from 'src/store/actions/clusterPage.actions';

const styles = {
  configureBtn: css`
    position: absolute;
    top: 12px;
    right: 12px;
  `
};

type ReplicasetServerListItemDropdownProps = {
  activeMaster?: boolean,
  replicasetUUID: string,
  showFailoverPromote?: boolean,
  uri: string,
  history: History,
  uuid: string
};

export class ReplicasetServerListItemDropdown extends React.PureComponent<
  ReplicasetServerListItemDropdownProps
> {
  render() {
    const {
      activeMaster,
      replicasetUUID,
      showFailoverPromote,
      uri,
      history,
      uuid
    } = this.props;

    return (
      <Dropdown
        items={[
          <DropdownItem onClick={() => history.push(`/cluster/dashboard/instance/${uuid}`)}>
            Server details
          </DropdownItem>,
          showFailoverPromote
            ? (
              <DropdownItem
                onClick={() => store.dispatch(
                  failoverPromoteLeader(replicasetUUID, uuid, activeMaster)
                )}
              >
                {activeMaster ? 'Force promote a leader' : 'Promote a leader'}
              </DropdownItem>
            )
            : null,
          <DropdownItem
            className={css`color: rgba(245, 34, 45, 0.65);`}
            onClick={() => store.dispatch(showExpelModal(uri))}
          >
            Expel server
          </DropdownItem>
        ]}
        className={cx(styles.configureBtn, 'meta-test__ReplicasetServerListItem__dropdownBtn')}
        popoverClassName='meta-test__ReplicasetServerListItem__dropdown'
      >
        <Button
          icon={IconMore}
          size='s'
          intent='plain'
        />
      </Dropdown>
    )
  }
}