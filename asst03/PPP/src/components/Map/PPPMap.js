/* eslint-disable no-undef */
// The line above fix google is undefined problem - don't delete
import React, { Component } from 'react';
import { withGoogleMap, GoogleMap } from 'react-google-maps'
import  PlaceMarker  from './PlaceMarker'
import uuid from 'uuid'

import hospitalIcon from "../../assets/icons/hospital.png"
import restaurantIcon from "../../assets/icons/restaurant.png"
import supermarketIcon from "../../assets/icons/supermarket.png"
import schoolIcon from "../../assets/icons/school.png"

const AirbnbMap = withGoogleMap(props => (
  <GoogleMap
    key="1"
    ref={props.onMapMounted}
    onZoomChanged={props.handleMapChanged}
    onDragEnd={props.handleMapChanged}
    onBoundsChanged={props.handleMapFullyLoaded}
    defaultCenter={props.center}
    defaultZoom={props.zoom}>
    {props.places}
</GoogleMap>
));


class PPPMap extends Component {

  constructor(props) {
    super(props)

    this.state = {
      lat: 50.0515918,
      lng: 19.9357531,
      places: [],
      init: false,
    };

    this.xMapBounds = { min: null, max: null }
    this.yMapBounds = { min: null, max: null }

    this.mapFullyLoaded = false
    this.zoom = 14

  }

  handleMapChanged() {
    this.getMapBounds()
    this.setMapCenterPoint()
    // this.fetchPlacesFromApi()
  }

  handleMapMounted(map) {
    this.map = map
  }

  handleMapFullyLoaded() {
    if (this.mapFullyLoaded)
      return

    this.mapFullyLoaded = true
    this.handleMapChanged()
  }

  setMapCenterPoint() {
    this.setState({
      lat: this.map.getCenter().lat(),
      lng: this.map.getCenter().lng()
    })
  }

  fetchPlacesFromApi() {
    const place = <PlaceMarker lat={50.0515918} lng={19.9357531} price={20} name={"Hotel"} description={"Hotel desc"} />
    this.setState({ places: [place] })
  }

  getMapBounds() {
    var mapBounds = this.map.getBounds()
    var xMapBounds = mapBounds.b
    var yMapBounds = mapBounds.f

    this.xMapBounds.min = xMapBounds.b
    this.xMapBounds.max = xMapBounds.f

    this.yMapBounds.min = yMapBounds.f
    this.yMapBounds.max = yMapBounds.b
  }

  extractPlaces(data) {
    var places = []
    data.restaurant.map( (restaurant) => {
      places.push(<PlaceMarker defaultIcon={restaurantIcon} key={uuid.v4()} id={uuid.v4()} lat={restaurant.location.lat} lng={restaurant.location.lng} name={restaurant.name} description={restaurant.vicinity} type="restaurant" object={restaurant}/>)
      return 1;
    })

    data.supermarket.map( (supermarket) => {
      places.push(<PlaceMarker defaultIcon={supermarketIcon} key={uuid.v4()} id={uuid.v4()} lat={supermarket.location.lat} lng={supermarket.location.lng} name={supermarket.name} description={supermarket.vicinity} type="supermarket" object={supermarket}/>)
      return 1;
    })

    data.school.map( (school) => {
      places.push(<PlaceMarker defaultIcon={schoolIcon} key={uuid.v4()} id={uuid.v4()} lat={school.location.lat} lng={school.location.lng} name={school.name} description={school.vicinity} type="school" object={school}/>)
      return 1;
    })

    data.hospital.map( (hospital) => {
      places.push(<PlaceMarker defaultIcon={hospitalIcon} key={uuid.v4()} id={uuid.v4()} lat={hospital.location.lat} lng={hospital.location.lng} name={hospital.name} description={hospital.vicinity} type="hospital" object={hospital}/>)
      return 1;
    })

    this.setState({
      places: places,
      lat: data.prediction.main.location.lat,
      lng: data.prediction.main.location.lng,
      init: true,
    })


  }

  async componentDidMount(){
    this.extractPlaces(this.props.data)
  }

  render() {
    const {lat, lng, places} = this.state;
    if (this.state.init === false) {
      return (
        <h1> loading... </h1>
      )
    } else {
      return (
       <div style={{ margin:'50px', width: `92%`, height: `750px`}}>
         <AirbnbMap
           center={{
             lat: lat,
             lng: lng
           }}
           zoom={this.zoom}
           containerElement={
             <div style={{ height: `100%` }} />
           }
           mapElement={
             <div style={{ height: `100%` }} />
           }
           places={places}
           onMapMounted={this.handleMapMounted.bind(this)}
           handleMapChanged={this.handleMapChanged.bind(this)}
           handleMapFullyLoaded={this.handleMapFullyLoaded.bind(this)}
         >
        </AirbnbMap>
      </div>
     );
    }

  }
}

export default (PPPMap)
