import Browser
import Html exposing (Html, button, div, text)
import Html.Events exposing (onClick)

import Element exposing (Element, el, text, row, alignRight, fill, width, rgb255, spacing, centerY, padding)
import Element.Background as Background
import Element.Border as Border
import Element.Font as Font

main =
  Browser.sandbox { init = init, update = update, view = view }

-- NOTE: what is here is just placeholder code while laying out the basic
-- structure of the client application. Most of this will change.

-- MODEL

type alias SubscriberModel = Int

init : SubscriberModel
init =
  0


-- UPDATE

type SubscriberCount = Increment | Decrement

update : SubscriberCount -> SubscriberModel -> SubscriberModel
update msg model =
  case msg of
    Increment ->
      model + 1
    Decrement ->
      model - 1


-- VIEW

view : SubscriberModel -> Html SubscriberCount
view model =
    Element.layout []
        topicInfo

topicInfo : Element msg
topicInfo =
    row [ width fill, centerY, spacing 30 ]
        [ topicCell "Application 1"
        , topicCell "1337"
        , topicCell "10"
        ]

topicCell : String -> Element msg
topicCell val =
    el
        [ Background.color (rgb255 240 0 245)
        , Font.color (rgb255 255 255 255)
        , Border.rounded 3
        , padding 30
        ]
        (text val)
