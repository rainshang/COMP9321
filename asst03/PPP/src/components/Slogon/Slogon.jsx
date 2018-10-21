
import React, { Component } from 'react';
import './Slogon.scss';
import { Button } from '@material-ui/core';
import LoginDialog from '../../pages/Auth/LoginDialog'
import {withRouter} from 'react-router-dom';

class Slogon extends Component {
  static displayName = 'Slogon';

  constructor(props) {
    super(props);
    this.state = {
      dialog: false
    }
  }

  openDialog() {
    var admin = JSON.parse(localStorage.getItem("admin"))
    if (admin != null) {
      this.props.history.push({
        pathname: `/metric`,
        state: {
          login: true
        }
      });
    } else {
      this.setState({
        dialog: true
      })
    }

  }

  closeDialog() {
    this.setState({
      dialog: false
    })
  }

  render() {
    return (
      <div className='slogon-view'
        style={this.props.style}>
        {this.state.dialog === true ? <LoginDialog key={"unique"} closeDialog={this.closeDialog.bind(this)}/> : null}
        <img className='app-logo'
          src={require('./images/logo.png')} />
        <div className='app-name'>
          Property Price Predictor
        </div>
        <div className='app-desc'>
          Trying to find a property to purchase in Melbourne?<br />
          Let us know what you're looking for and<br />
          we'll suggest how much it will cost to but your dream home!<br />
          Or see trending suburb <br />
        <Button className='app-more'
          color='primary'
          onClick={this.openDialog.bind(this)}
          >
          See more
        </Button>
        </div>
      </div>
    );
  }
}
export default withRouter(Slogon);
