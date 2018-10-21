import React, { Component } from 'react'
import { Marker } from 'react-google-maps'
import PlaceInfoWindow from './PlaceInfoWindow'
import {MapConsumer} from './MapContext'

export class PlaceMarker extends Component {
  constructor(props) {
    super(props)

     this.state = {
      showTooltip: false
    }
  }
  clickTooltip(dispatch) {
    const {lat,lng} = this.props;
    dispatch({
      type: 'OPEN_INFOWINDOW',
      payload: lat+lng,
    })
    this.setState({ showTooltip: !this.state.showTooltip })
  }

  closeWindow(dispatch) {
    dispatch({
      type: 'CLOSE_INFOWINDOW',
    })
  }

  render() {
    const {lat, lng, name, description, type, object} = this.props
    const total = lat + lng;
    const {showTooltip} = this.state
    return(
      <MapConsumer>
        {value => {
          const {dispatch, infoWindow} = value
          return (
            <Marker
              position={{
                lat: parseFloat(lat),
                lng: parseFloat(lng)
              }}
              defaultIcon={this.props.defaultIcon}
              onClick={this.clickTooltip.bind(this, dispatch)}
            >
            {total === infoWindow && (
              <PlaceInfoWindow description={description}
                               name={name}
                               type={type}
                               object={object}
                               closeWindow={this.closeWindow.bind(this, dispatch)}/>
            )}
            </Marker>
          )
        }}
      </MapConsumer>

    );
  }
}

PlaceMarker.defaultProps = {
  price: ""
}

export default PlaceMarker
