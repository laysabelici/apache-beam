{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyNvL91NT3qXUOMC8UQLSYH0",
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/laysabelici/apache-beam/blob/main/apache_beam_write_outputs.ipynb\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "#INSTALADOR PARA MÁQUINA VIRTUAL, EM MÁQUINA LOCAL NÃO COLOQUE [INTERATIVE]\n",
        "pip install apache_beam[interative]"
      ],
      "metadata": {
        "id": "hwWfFO_dnwBq"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "#IMPORT BIBLIOTECA\n",
        "import apache_beam as beam"
      ],
      "metadata": {
        "id": "E3PTrdU6n8RQ"
      },
      "execution_count": 5,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "#DEFINIR A PIPELINE\n",
        "p1 = beam.Pipeline()\n",
        "\n",
        "voos = (\n",
        "    p1\n",
        "    #LER ARQUIVO E EXCLUIR CABEÇALHO\n",
        "    #AS PIPES >> SIGNIFICAM QUE UM COMANDO É USADO COMO INPUT DO OUTRO\n",
        "    |\"Importar dados\" >> beam.io.ReadFromText('/content/voos_sample.csv', skip_header_lines = 1)\n",
        "    #aplicando a função lambda a cada elemento do PCollection (coleção de dados)\n",
        "    #representada por record.\n",
        "    #A função lambda record.split(',') está dividindo cada registro por vírgulas, criando assim uma lista de valores.\n",
        "    |\"Separar por vírgula\" >> beam.Map(lambda record: record.split(','))\n",
        "    #Gravar de dados\n",
        "    |\"Gravar dados\" >> beam.io.WriteToText('/content/voos.txt')\n",
        ")\n",
        "\n",
        "p1.run()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "jIxRFd0Kp6az",
        "outputId": "71980356-4db8-4647-98bc-35c43c44e761"
      },
      "execution_count": 19,
      "outputs": [
        {
          "output_type": "execute_result",
          "data": {
            "text/plain": [
              "<apache_beam.runners.portability.fn_api_runner.fn_runner.RunnerResult at 0x7c91fc775750>"
            ]
          },
          "metadata": {},
          "execution_count": 19
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [],
      "metadata": {
        "id": "1OJVcD2Xt1i2"
      },
      "execution_count": null,
      "outputs": []
    }
  ]
}
