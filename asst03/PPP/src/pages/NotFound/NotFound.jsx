import React, { Component } from 'react';
import { Link } from 'react-router-dom';
import './NotFound.scss';

export default class NotFound extends Component {
  static displayName = 'NotFound';

  constructor(props) {
    super(props);
    this.state = {};
  }

  render() {
    return (
      <div className="basic-not-found">
        <img
          src={require('./images/TB1txw7bNrI8KJjy0FpXXb5hVXa-260-260.png')}
          className="imgException"
        />
        <div className="prompt">
          <h3 className="title">
            Sorry, the page not found
          </h3>
          <p className="description">
            The page you need is not found. Please back to <Link to="/">homepage</Link> to continue
          </p>
        </div>
      </div>
    );
  }
}
