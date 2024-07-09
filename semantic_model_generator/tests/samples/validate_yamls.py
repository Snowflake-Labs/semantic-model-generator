from semantic_model_generator.protos import semantic_model_pb2

_VALID_YAML = """name: my test semantic model
tables:
  - name: ALIAS
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: ALIAS
    filters:
      - name: '  ' # <FILL-OUT>
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: '  ' # <FILL-OUT>
    dimensions:
      - name: ALIAS
        synonyms:
            - 'an alias for something'
        expr: ALIAS
        data_type: TEXT
        sample_values:
          - Holtsville
          - Adjuntas
          - Boqueron
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
_LONG_VQR_CONTEXT = """
  - name: "Max spend"
    question: "Over the past week what was spend?"
    verified_at: 1714497970
    verified_by: jonathan
    sql: "
  WITH Minute_Usage AS (
  SELECT
    DATE_TRUNC('MINUTE', Timestamp_Column) AS Minute,
    SUM(Credits_Column) AS Minute_Total_Spend
  FROM Your_Table_Name
  WHERE
    Timestamp_Column >= DATEADD(DAY, -10, CURRENT_DATE)
  GROUP BY
    Minute
)
SELECT
  Minute,
  MAX(Minute_Total_Spend) AS Max_Credits
FROM Minute_Usage
GROUP BY
  Minute
ORDER BY
  Minute DESC;
    "
"""
_VALID_YAML_LONG_VQR_CONTEXT = (
    """name: my test semantic model
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
          - Adjuntas
          - Boqueron
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
verified_queries:
"""
    + _LONG_VQR_CONTEXT * 100
)


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
        - Holtsville
        - Adjuntas
        - Boqueron
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
          - Holtsville
          - Adjuntas
          - Boqueron
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
          - Holtsville
          - Adjuntas
          - Boqueron
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
          - Holtsville
          - Adjuntas
          - Boqueron
    measures:
      - name: ZIP_CODE
        synonyms:
            - 'another synonym'
        expr: ZIP_CODE
        data_type: OBJECT
        sample_values:
          - '{1:2}'
"""


_INVALID_YAML_TOO_LONG_CONTEXT = """name: my test semantic model
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
          - Adjuntas
          - Boqueron
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

_VALID_YAML_FLOW_STYLE = """name: my test semantic model
tables:
  - name: ALIAS
    base_table:
      database: AUTOSQL_DATASET_BIRD_V2
      schema: ADDRESS
      table: ALIAS
    dimensions:
      - name: ALIAS
        synonyms: ['an alias for something']
        expr: ALIAS
        data_type: TEXT
        sample_values: ['Holtsville', 'Adjuntas', 'Boqueron']
"""

_VALID_YAML_MANY_SAMPLE_VALUES = semantic_model_pb2.SemanticModel(
    name="test model",
    tables=[
        semantic_model_pb2.Table(
            name="ALIAS",
            base_table=semantic_model_pb2.FullyQualifiedTable(
                database="AUTOSQL_DATASET_BIRD_V2", schema="ADDRESS", table="ALIAS"
            ),
            dimensions=[
                semantic_model_pb2.Dimension(
                    name=f"DIMENSION_{i}",
                    expr="ALIAS",
                    data_type="TEXT",
                    sample_values=[
                        "apple",
                        "banana",
                        "cantaloupe",
                        "date",
                        "elderberry",
                    ]
                    * 100,
                )
                for i in range(5)
            ],
        )
    ],
)
