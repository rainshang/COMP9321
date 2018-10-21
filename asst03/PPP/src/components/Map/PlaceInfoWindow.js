import React, { Component } from 'react'
import { InfoWindow } from 'react-google-maps'
import uuid from 'uuid'

export class PlaceInfoWindow extends Component {
  render() {
    const {description, name, type, object} = this.props
    var img = null;
    if (type==="restaurant" || type==="supermarket") {
      img = (
        <div key={uuid.v4()}>
          <img src={object.photo} height="150" width="200" alt="error" />
        </div>
      )
    }

    return(
      <InfoWindow onCloseClick={this.props.closeWindow}>
        <div>
          <h1>{name}</h1>
          <p>{description}</p>
          {img}
        </div>
      </InfoWindow>
    );
  }
}



export default PlaceInfoWindow
