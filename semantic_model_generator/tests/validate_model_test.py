import tempfile
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from semantic_model_generator.validate_model import validate


@pytest.fixture
def mock_snowflake_connection():
    """Fixture to mock the snowflake_connection function."""
    with patch("semantic_model_generator.validate_model.SnowflakeConnector") as mock:
        mock.return_value = MagicMock()
        yield mock


_VALID_YAML = """name: my test semantic model
tables:
  - name: ALIAS
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: ALIAS
    dimensions:
      - name: ALIAS
        synonyms:
            - 'an alias for something'
        expr: ALIAS
        data_type: TEXT
        sample_values:
          - "Holtsville"
          - "Adjuntas"
          - "Boqueron"
    measures:
      - name: ZIP_CODE
        synonyms:
            - 'another synonym'
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
  - name: AREA_CODE
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: AREA_CODE
    measures:
      - name: ZIP_CODE
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
          - '544'
      - name: AREA_CODE
        expr: AREA_CODE
        data_type: NUMBER
        sample_values:
          - '631'
"""

_INVALID_YAML_FORMATTING = """name: my test semantic model
tables:
  - name: ALIAS
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: ALIAS
    dimensions:
    - name: ALIAS
    synonyms:
        - 'an alias for something'
    expr: ALIAS
    data_type: TEXT
    sample_values:
        - "Holtsville"
        - "Adjuntas"
        - "Boqueron"
    measures:
    - name: ZIP_CODE
    synonyms:
        - 'another synonym'
    expr: ZIP_CODE
    data_type: NUMBER
    sample_values:
        - '501'
  - name: AREA_CODE
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: AREA_CODE
    measures:
      - name: ZIP_CODE
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
          - '544'
      - name: AREA_CODE
        expr: AREA_CODE
        data_type: NUMBER
        sample_values:
          - '631'
"""


_INVALID_YAML_UPPERCASE_DEFAULT_AGG = """name: my test semantic model
tables:
  - name: ALIAS
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: ALIAS
    dimensions:
      - name: ALIAS
        synonyms:
            - 'an alias for something'
        expr: ALIAS
        data_type: TEXT
        sample_values:
          - "Holtsville"
          - "Adjuntas"
          - "Boqueron"
    measures:
      - name: ZIP_CODE
        synonyms:
            - 'another synonym'
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
        default_aggregation: AVG
  - name: AREA_CODE
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: AREA_CODE
    measures:
      - name: ZIP_CODE
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
          - '544'
      - name: AREA_CODE
        expr: AREA_CODE
        data_type: NUMBER
        sample_values:
          - '631'
"""

_INVALID_YAML_UNMATCHED_QUOTE = """name: my test semantic model
tables:
  - name: ALIAS
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: ALIAS
    dimensions:
      - name: ALIAS
        synonyms:
            - 'an alias for something'
        expr: ALIAS
        data_type: TEXT
        sample_values:
          - "Holtsville"
          - "Adjuntas"
          - "Boqueron"
    measures:
      - name: ZIP_CODE
        synonyms:
            - 'another synonym'
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
  - name: AREA_CODE
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: AREA_CODE
    measures:
      - name: ZIP_CODE"
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
          - '544'
      - name: AREA_CODE
        expr: AREA_CODE
        data_type: NUMBER
        sample_values:
          - '631'
"""


_INVALID_YAML_INCORRECT_DATA_TYPE = """name: my test semantic model
tables:
  - name: ALIAS
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: ALIAS
    dimensions:
      - name: ALIAS
        synonyms:
            - 'an alias for something'
        expr: ALIAS
        data_type: TEXT
        sample_values:
          - 'Holtsville'
          - "Adjuntas"
          - "Boqueron"
    measures:
      - name: ZIP_CODE
        synonyms:
            - 'another synonym'
        expr: ZIP_CODE
        data_type: OBJECT
        sample_values:
          - '{1:2}'
"""


_VALID_YAML_TOO_LONG_CONTEXT = """name: my test semantic model
tables:
  - name: ALIAS
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: ALIAS
    dimensions:
      - name: ALIAS
        synonyms:
            - 'an alias for something'
        expr: ALIAS
        data_type: TEXT
        sample_values:
          - "Holtsville"
          - "Adjuntas"
          - "Boqueron"
        description: The world is a vast and diverse place, filled with an array of landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities.  f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities. f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities.  d is a vast and diverse place, filled with an array of landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities.  f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities. f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities.   The world is a vast and diverse place, filled with an array of landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities.  f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities. f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities.  d is a vast and diverse place, filled with an array of landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities.  f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities. f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities.   The world is a vast and diverse place, filled with an array of landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities.  f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities. f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities.  d is a vast and diverse place, filled with an array of landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities.  f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communities. f landscapes, cultures, and ecosystems. From the towering peaks of the Himalayas to the depths of the Amazon rainforest, the Earth is home to a rich tapestry of natural wonders. Inhabitants of the world span a spectrum of species, from microscopic organisms thriving in the depths of the ocean to majestic creatures roaming the savannahs of Africa. Human civilization has flourished across continents, giving rise to an intricate tapestry of languages, traditions, and beliefs. The world's history is a story of triumphs and tragedies, marked by epochs of innovation and exploration alongside periods of conflict and upheaval. From the ancient civilizations of Mesopotamia and Egypt to the rise and fall of empires like Rome and Byzantium, the past has shaped the present in profound ways. Today, the world is interconnected as never before, with advances in technology and communication bridging distances and connecting people from every corner of the globe. Globalization has brought both opportunities and challenges, transforming economies, societies, and the environment in its wake. As we navigate the complexities of the modern world, we are confronted with urgent issues such as climate change, poverty, and inequality. Yet, amid these challenges, there is also hope – in the resilience of communities, the ingenuity of innovators, and the collective efforts of individuals striving for a better future. In this ever-evolving world, each day brings new discoveries, new connections, and new possibilities. It is a world of boundless beauty and complexity, waiting to be explored and understood, and it is our collective responsibility to cherish and steward it for generations to come. From the bustling streets of metropolises to the quiet serenity of remote villages, the world offers a mosaic of lifestyles and experiences. Cultural diversity enriches our understanding of humanity, with traditions, art forms, and cuisines reflecting the unique identities of different communit
    measures:
      - name: ZIP_CODE
        synonyms:
            - 'another synonym'
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
  - name: AREA_CODE
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: AREA_CODE
    measures:
      - name: ZIP_CODE
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '501'
          - '544'
      - name: AREA_CODE
        expr: AREA_CODE
        data_type: NUMBER
        sample_values:
          - '631'

"""


_INVALID_YAML_MISSING_QUOTES_SAMPLE_VALUES = """name: my test semantic model
tables:
  - name: ALIAS
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: ALIAS
    dimensions:
      - name: ALIAS
        synonyms:
            - 'an alias for something'
        expr: ALIAS
        data_type: TEXT
        sample_values:
          - Holtsville
          - "Adjuntas"
          - "Boqueron"
    measures:
      - name: ZIP_CODE
        synonyms:
            - 'another synonym'
        expr: ZIP_CODE
        data_type: NUMBER
        sample_values:
          - '234324'
"""


@pytest.fixture
def temp_valid_yaml_file():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(_VALID_YAML)
        tmp.flush()  # Ensure all data is written to the file
        yield tmp.name


@pytest.fixture
def temp_invalid_yaml_formatting_file():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(_INVALID_YAML_FORMATTING)
        tmp.flush()
        yield tmp.name


@pytest.fixture
def temp_invalid_yaml_uppercase_file():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(_INVALID_YAML_UPPERCASE_DEFAULT_AGG)
        tmp.flush()
        yield tmp.name


@pytest.fixture
def temp_invalid_yaml_unmatched_quote_file():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(_INVALID_YAML_UNMATCHED_QUOTE)
        tmp.flush()
        yield tmp.name


@pytest.fixture
def temp_invalid_yaml_incorrect_dtype():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(_INVALID_YAML_INCORRECT_DATA_TYPE)
        tmp.flush()
        yield tmp.name


@pytest.fixture
def temp_valid_yaml_too_long_context():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(_VALID_YAML_TOO_LONG_CONTEXT)
        tmp.flush()
        yield tmp.name


@pytest.fixture
def temp_valid_yaml_missing_quotes():
    """Create a temporary YAML file with the test data."""
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp:
        tmp.write(_INVALID_YAML_MISSING_QUOTES_SAMPLE_VALUES)
        tmp.flush()
        yield tmp.name


@mock.patch("semantic_model_generator.validate_model.logger")
def test_valid_yaml(mock_logger, temp_valid_yaml_file, mock_snowflake_connection):
    account_name = "snowflake test"

    validate(temp_valid_yaml_file, account_name)

    expected_log_call_1 = mock.call.info(
        f"Successfully validated {temp_valid_yaml_file}"
    )
    expected_log_call_2 = mock.call.info("Checking logical table: ALIAS")
    expected_log_call_3 = mock.call.info("Validated logical table: ALIAS")
    assert (
        expected_log_call_1 in mock_logger.mock_calls
    ), "Expected log message not found in logger calls"
    assert (
        expected_log_call_2 in mock_logger.mock_calls
    ), "Expected log message not found in logger calls"
    assert (
        expected_log_call_3 in mock_logger.mock_calls
    ), "Expected log message not found in logger calls"
    snowflake_query_one = (
        "SELECT ALIAS, ZIP_CODE FROM AUTOSQL_DATASET_BIRD_V2.ADDRESS.ALIAS LIMIT 100"
    )
    snowflake_query_two = (
        "SELECT ALIAS, ZIP_CODE FROM AUTOSQL_DATASET_BIRD_V2.ADDRESS.ALIAS LIMIT 100"
    )
    assert any(
        snowflake_query_one in str(call)
        for call in mock_snowflake_connection.mock_calls
    ), "Query not executed"
    assert any(
        snowflake_query_two in str(call)
        for call in mock_snowflake_connection.mock_calls
    ), "Query not executed"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_formatting(mock_logger, temp_invalid_yaml_formatting_file):
    account_name = "snowflake test"
    with pytest.raises(ValueError) as exc_info:
        validate(temp_invalid_yaml_formatting_file, account_name)

    expected_error_fragment = (
        "Failed to parse tables field: "
        'Message type "semantic_model_generator.Table" has no field named "expr" at "SemanticModel.tables[0]".'
    )
    assert expected_error_fragment in str(exc_info.value), "Unexpected error message"

    expected_log_call = mock.call.info(
        f"Successfully validated {temp_invalid_yaml_formatting_file}"
    )
    assert (
        expected_log_call not in mock_logger.mock_calls
    ), "Unexpected log message found in logger calls"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_uppercase(mock_logger, temp_invalid_yaml_uppercase_file):
    account_name = "snowflake test"
    with pytest.raises(ValueError) as exc_info:
        validate(temp_invalid_yaml_uppercase_file, account_name)

    expected_error_fragment = "Unable to parse yaml to protobuf. Error: Failed to parse tables field: Failed to parse measures field: Failed to parse default_aggregation field: Invalid enum value AVG for enum type semantic_model_generator.AggregationType at SemanticModel.tables[0].measures[0].default_aggregation..."
    assert expected_error_fragment in str(exc_info.value), "Unexpected error message"

    expected_log_call = mock.call.info(
        f"Successfully validated {temp_invalid_yaml_uppercase_file}"
    )
    assert (
        expected_log_call not in mock_logger.mock_calls
    ), "Unexpected log message found in logger calls"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_missing_quote(
    mock_logger, temp_invalid_yaml_unmatched_quote_file, mock_snowflake_connection
):
    account_name = "snowflake test"
    with pytest.raises(ValueError) as exc_info:
        validate(temp_invalid_yaml_unmatched_quote_file, account_name)

    expected_error_fragment = "Unable to validate your semantic model. Error = Invalid column name 'ZIP_CODE\"'. Mismatched quotes detected."

    assert expected_error_fragment in str(exc_info.value), "Unexpected error message"

    expected_log_call = mock.call.info(
        f"Successfully validated {temp_invalid_yaml_unmatched_quote_file}"
    )

    assert (
        expected_log_call not in mock_logger.mock_calls
    ), "Unexpected log message found in logger calls"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_invalid_yaml_incorrect_datatype(
    mock_logger, temp_invalid_yaml_incorrect_dtype, mock_snowflake_connection
):
    account_name = "snowflake test"
    with pytest.raises(ValueError) as exc_info:
        validate(temp_invalid_yaml_incorrect_dtype, account_name)

    expected_error = "Unable to validate your semantic model. Error = We do not support object datatypes in the semantic model. Col ZIP_CODE has data type OBJECT. Please remove this column from your semantic model."

    assert expected_error in str(exc_info.value), "Unexpected error message"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_valid_yaml_too_long_context(
    mock_logger, temp_valid_yaml_too_long_context, mock_snowflake_connection
):
    account_name = "snowflake test"
    with pytest.raises(ValueError) as exc_info:
        validate(temp_valid_yaml_too_long_context, account_name)

    expected_error = "Your semantic model is too large. Passed size is 37222 characters. We need you to remove 13140 characters in your semantic model. Please check: \n (1) If you have long descriptions that can be truncated. \n (2) If you can remove some columns that are not used within your tables. \n (3) If you have extra tables you do not need. \n (4) If you can remove sample values."

    assert expected_error in str(exc_info.value), "Unexpected error message"


@mock.patch("semantic_model_generator.validate_model.logger")
def test_valid_yaml_missing_quotes(
    mock_logger, temp_valid_yaml_missing_quotes, mock_snowflake_connection
):
    account_name = "snowflake test"
    with pytest.raises(ValueError) as exc_info:
        validate(temp_valid_yaml_missing_quotes, account_name)

    expected_error = "You need to have all sample_values: surrounded by quotes. Please fix the value - Holtsville."

    assert expected_error in str(exc_info.value), "Unexpected error message"
