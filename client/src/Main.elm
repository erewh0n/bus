import Browser
import Browser.Navigation as Nav
import Html exposing (Html, button, div, text)
import Html.Events exposing (onClick)
import Http
import Json.Decode as Decode
import List
import Url
import Array

import Element exposing (Element, el, text, row, column, alignRight, fill, width, rgb255, spacing, centerY, padding)
import Element.Background as Background
import Element.Border as Border
import Element.Font as Font
import Element.Input as Input

main =
  Browser.application
    { init = init
    , view = view
    , update = update
    , subscriptions = subscriptions
    , onUrlChange = LinkClicked
    , onUrlRequest = UrlChanged
    }

-- MODEL

type alias Topics = List String
type alias Topic = String

type alias Status =
    { server_id : String
    , topics : Topics
    }

type alias Connection =
    { name : String
    }

unknown = (Status "Not connected" [])

init : () -> Url.Url -> Nav.Key -> ( Status, Cmd Msg )
init flags url key =
    ( unknown, Cmd.none )

-- HTTP

getStatus : Cmd Msg
getStatus = Http.send StatusReceived
    (Http.get "http://localhost:8222/connz" decodeStatus)

decodeStatus : Decode.Decoder Status
decodeStatus =
    Decode.map2 Status
        (Decode.field "server_id" Decode.string)
        (Decode.field "connections" (Decode.list decodeConnectionEntry) )

decodeConnectionEntry : Decode.Decoder Topic
decodeConnectionEntry =
    Decode.field "name" Decode.string

-- UPDATE

type Msg 
    = Click
    | StatusReceived (Result Http.Error Status) 
    | LinkClicked Url.Url
    | UrlChanged Browser.UrlRequest

update : Msg -> Status -> (Status, Cmd Msg)
update msg status =
  case msg of
    Click ->
        (status, getStatus)
    StatusReceived (Ok statusUpdates) ->
        (statusUpdates, Cmd.none)
    StatusReceived (Err httpError) ->
        ({unknown | server_id = (extractError httpError)}, Cmd.none)
    LinkClicked urlRequest ->
        (unknown, Cmd.none)
    UrlChanged urlRequest ->
        case urlRequest of
            Browser.Internal url ->
                ({ status | server_id = (Url.toString url) }
                , Cmd.none
                )
            Browser.External url ->
                ( status
                , Nav.load url
                )

extractError : Http.Error -> String
extractError error =
    case error of
        Http.BadUrl text ->
            "Bad Url: " ++ text
        Http.Timeout ->
            "Http Timeout"
        Http.NetworkError ->
            "Network Error"
        Http.BadStatus response ->
            "Bad Http Status: " ++ String.fromInt response.status.code
        Http.BadPayload message response ->
            "Bad Http Payload: "
                ++ String.fromInt response.status.code
                ++ " "
                ++ message

-- SUBSCRIPTIONS

subscriptions : Status -> Sub Msg
subscriptions _ =
  Sub.none

-- VIEW
view : Status -> Browser.Document Msg
view status =
    { title = "The Bus!"
    , body =
        [ Element.layout [
            Background.color (rgb255 40 40 40)
        ]
            (column []
                [ topicStatus status.server_id
                , column []
                    (List.map topicEntry status.topics)
                ]
            )
        ]
    }

topicEntry : Topic -> Element Msg
topicEntry topic =
    row [ width fill
        , centerY
        , spacing 30 ]
        [ topicCell "Name"
        , topicCell topic
        ]

button : String -> Msg -> Element Msg
button buttonText msg =
    Input.button
        [ Font.color (rgb255 255 255 255)
        , Background.color (rgb255 80 80 80)
        , Border.rounded 10
        , padding 10
        , width (Element.px 100)
        , Font.center
        , Border.width 2
        ]
        { onPress = Just msg
        , label = text buttonText
        }

topicStatus : String -> Element Msg
topicStatus name =
    row [ width fill
        , centerY
        , spacing 10
        , padding 10
        ]
        [ button "Refresh" Click
        , topicCell name
        ]

topicCell : String -> Element msg
topicCell val =
    el
        [ Background.color (rgb255 40 40 40)
        , Font.color (rgb255 255 255 255)
        , Border.rounded 10
        , padding 30
        ]
        (text val)
