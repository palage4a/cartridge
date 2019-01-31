import * as React from "react";
import Modal from 'src/components/Modal';
import {connect} from 'react-redux';
import {changeFailover, setVisibleFailoverModal} from "../../store/actions/clusterPage.actions";

export default connect(({app, ui}) => {
  return {
    clusterSelf: app.clusterSelf,
    failover: app.failover,
    showFailoverModal: ui.showFailoverModal,
  }
})(
class FailoverButton extends React.Component {

  render() {
    if (!this.props.clusterSelf.configured)
      return null;
    return <React.Fragment>
      <button
          type="button"
          className="btn btn-light btn-sm"
          onClick={() => this.props.dispatch(setVisibleFailoverModal(true))}
        >
          Failover:{' '}<span>{this.props.failover ? 'enabled' : 'disabled'}</span>
        </button>
      <Modal
        title={'Failover control'}
        visible={this.props.showFailoverModal}
        width={540}
        onOk={() => this.props.dispatch(changeFailover({enabled: !this.props.failover}))}
        okText={this.props.failover ? 'Disable' : 'Enable'}
        onCancel={() => this.props.dispatch(setVisibleFailoverModal(false))}
        cancelText={'Close'}
      >
        <p>Current status:{' '}<b>{this.props.failover ? 'enabled' : 'disabled'}</b>.</p>
        <p>When enabled, every storage starts monitoring instance statuses. If a user-specified master goes down, a replica with the lowest UUID takes its place. When the user-specified master comes back online, both roles are restored.</p>
      </Modal>
    </React.Fragment>;
  }
}
)
