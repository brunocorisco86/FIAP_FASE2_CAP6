# encoding: utf8

import os
import oracledb
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate

# Constantes para cálculos
DIESEL_POR_HECTARE = 60  # litros

EMISSAO_CO2_POR_LITRO = 2.65  # kg de CO2 por litro de diesel
#Fonte: https://revistaft.com.br/analise-de-consumo-e-produtividade-de-colhedoras-de-cana-com-extratores-primarios-de-materiais-diferentes/#:~:text=Conforme%20descrito%20pela%20Revista%20Cultiva,de%20cana%2Dde%2Da%C3%A7%C3%BAcar.

PRECO_DIESEL = 6.01  # R$ por litro
# Fonte: https://precos.petrobras.com.br/sele%C3%A7%C3%A3o-de-estados-diesel


# Dados para cálculo de perdas
perda_velocidade = {3: 2.25, 4: 2.4, 5: 2.22, 6: 4.16, 7: 4.84, 8: 4.59}
perda_rotacao = {800: 2.27, 900: 2.3, 1000: 2.35, 1100: 2.4}
perda_horas = {2: 3.0, 4: 4.2, 6: 5.3, 8: 6.5}
perda_colheitadeira = {'A': 1.25, 'B': 2.1, 'C': 2.45}


def estimar_tempo_colheita(area_em_hectares, largura_maquina, velocidade_km_h):
    area_em_metros_quadrados = area_em_hectares * 10000
    tempo_colheita_sem_paradas = area_em_metros_quadrados / (largura_maquina * velocidade_km_h * 1000)
    num_paradas = int(tempo_colheita_sem_paradas // 8)
    tempo_paradas = num_paradas * 1
    tempo_colheita_total = tempo_colheita_sem_paradas + tempo_paradas
    return tempo_colheita_total


def estimar_perdas(velocidade, rotacao, horas_sem_afiacao, colheitadeira):
    perda_total = (
        perda_velocidade.get(velocidade, 0) +
        perda_rotacao.get(rotacao, 0) +
        perda_horas.get(horas_sem_afiacao, 0) +
        perda_colheitadeira.get(colheitadeira, 0)
    )
    return perda_total


def calcular_consumo_diesel(area):
    return area * DIESEL_POR_HECTARE


def calcular_emissao_co2(consumo_diesel):
    return consumo_diesel * EMISSAO_CO2_POR_LITRO


def calcular_custo_diesel(consumo_diesel):
    return consumo_diesel * PRECO_DIESEL


if __name__ == "__main__":

    try:
        with oracledb.connect(user='rm560518', password="230186", dsn='oracle.fiap.com.br:1521/ORCL') as \
            connection:

            with connection.cursor() as cursor:

                id = None

                while True:
                    print("Obrigado por utilizar o Sistema de Colheita de Cana-de-Açúcar!")

                    os.system('cls')

                    print('------- SISTEMA DE COLHEITA DE CANA-DE-AÇÚCAR -------')
                    print('')
                    print("""
                    1 - Cadastrar Operação de Colheita
                    2 - Listar Operações
                    3 - Alterar Operação
                    4 - Excluir Operação
                    5 - Estimar Perdas, Tempo e Custo de Colheita
                    6 - Consultar Operação Específica
                    7 - SAIR
                    """)

                    margem = ' ' * 4

                    escolha = input(margem + "Escolha -> ")

                    if escolha.isdigit():
                        escolha = int(escolha)
                    else:
                        escolha = 7

                        print("Digite um número.\nReinicie a Aplicação!")

                    os.system('cls')

                    match escolha:
                        case 1:
                            try:
                                
                                print("----- CADASTRAR OPERAÇÃO DE COLHEITA -----\n")
                                area = float(input(margem + "Área em hectares: "))
                                velocidade = int(input(margem + "Velocidade de deslocamento (km/h): "))
                                rotacao = int(input(margem + "Rotação do Exaustor Primário: "))
                                horas_sem_afiacao = int(input(margem + "Horas sem afiação da faca de corte: "))
                                colheitadeira = input(margem + "Modelo da Colheitadeira (A, B ou C): ").upper()
                                largura_maquina = float(input(margem + "Largura da máquina (m): "))
                                data_inicio = input(margem + "Data de início da colheita (DD/MM/AAAA): ")
                                perdas = estimar_perdas(velocidade, rotacao, horas_sem_afiacao, colheitadeira)
                                tempo_colheita = estimar_tempo_colheita(area, largura_maquina, velocidade)
                                consumo_diesel = calcular_consumo_diesel(area)
                                emissao_co2 = calcular_emissao_co2(consumo_diesel)
                                custo_diesel = calcular_custo_diesel(consumo_diesel)
                                data_inicio = datetime.strptime(data_inicio, "%d/%m/%Y")
                                data_fim = data_inicio + timedelta(hours=tempo_colheita)

                                id = int(round(datetime.now().timestamp()))
 
                                cadastro = \
                                    f"""
                                    insert into
                                        COLHEITA_CANA (
                                            id,
                                            area,
                                            velocidade,
                                            rotacao,
                                            horas_sem_afiacao,
                                            colheitadeira,
                                            largura_maquina, 
                                            perdas_estimadas,
                                            tempo_estimado,
                                            consumo_diesel,
                                            emissao_co2,
                                            custo_diesel,
                                            data_inicio,
                                            data_fim
                                        )
                                        VALUES (
                                            {str(id)},
                                            {area},
                                            {velocidade}, 
                                            {rotacao},
                                            {horas_sem_afiacao},
                                            '{colheitadeira}',
                                            {largura_maquina}, 
                                            {perdas},
                                            {tempo_colheita},
                                            {consumo_diesel},
                                            {emissao_co2},
                                            {custo_diesel},
                                            TO_DATE('{data_inicio.strftime("%d/%m/%Y")}', 'DD/MM/YYYY'), 
                                            TO_DATE('{data_fim.strftime("%d/%m/%Y")}', 'DD/MM/YYYY')
                                        )
                                    """.strip()

                                cursor.execute(cadastro)
                                connection.commit()

                            except ValueError:
                                print("Erro: Certifique-se de inserir valores numéricos corretos.")

                            except Exception as e:
                                print(f"Erro na transação do BD: {e}")

                            else:
                                print("\n##### Operação de Colheita CADASTRADA #####")
                                print(f"Perdas estimadas: {perdas:.2f}%")
                                print(f"Tempo estimado de colheita: {tempo_colheita:.2f} horas")
                                print(f"Consumo estimado de diesel: {consumo_diesel:.2f} litros")
                                print(f"Emissão estimada de CO2: {emissao_co2:.2f} kg")
                                print(f"Custo estimado de diesel: R$ {custo_diesel:.2f}")
                                print(f"Data de início: {data_inicio.strftime('%d/%m/%Y')}")
                                print(f"Data de fim estimada: {data_fim.strftime('%d/%m/%Y')}")

                        case 2:
                                if not id:
                                    print(f'Volte para a opcao 1')

                                else:
                                    print("----- LISTAR OPERAÇÕES DE COLHEITA -----\n")

                                    cursor.execute(f'SELECT * FROM COLHEITA_CANA WHERE ID={id}')
                                    data = cursor.fetchone()

                                    if not data:
                                        print("Não há operações de colheita cadastradas!")

                                    else:
                                        print(tabulate(
                                            pd.DataFrame(
                                                [data],
                                                columns= [
                                                    'ID',
                                                    'Área',
                                                    'Velocidade',
                                                    'Rotação',
                                                    'Horas sem Afiação',
                                                    'Colheitadeira',
                                                    'Largura Máquina',
                                                    'Perdas Estimadas',
                                                    'Tempo Estimado',
                                                    'Consumo Diesel',
                                                    'Emissão CO2',
                                                    'Custo Diesel',
                                                    'Data Início',
                                                    'Data Fim'
                                                ]
                                            ),
                                            headers='keys',
                                            tablefmt='psql',
                                            showindex='never'
                                        ))

                                        print("\nPerdas Estimadas em %, Tempo Estimado em horas, Consumo Diesel em litros, Emissão CO2 em kg, Custo Diesel em R$.")

                        case 3:
                            print("----- ALTERAR OPERAÇÃO DE COLHEITA -----\n")
                            op_id = int(input(margem + "ID da operação a ser alterada: "))

                        case 4:
                            # Implementar lógica de alteração similar ao cadastro
                            print("----- EXCLUIR OPERAÇÃO DE COLHEITA -----\n")

                            op_id = int(input(margem + "ID da operação a ser excluída: "))

                            cursor.execute(f"DELETE FROM COLHEITA_CANA WHERE id={op_id}")
                            connection.commit()

                            print("\n##### Operação EXCLUÍDA! #####")

                        case 5:
                            print("----- ESTIMAR PERDAS, TEMPO E CUSTO DE COLHEITA -----\n")

                            area = float(input(margem + "Área em hectares: "))
                            velocidade = int(input(margem + "Velocidade de deslocamento (km/h): "))
                            rotacao = int(input(margem + "Rotação do Exaustor Primário: "))
                            horas_sem_afiacao = int(input(margem + "Horas sem afiação da faca de corte: "))
                            colheitadeira = input(margem + "Modelo da Colheitadeira (A, B ou C): ").upper()
                            largura_maquina = float(input(margem + "Largura da máquina (m): "))
                            #calcular
                            perdas = estimar_perdas(velocidade, rotacao, horas_sem_afiacao, colheitadeira)
                            tempo_colheita = estimar_tempo_colheita(area, largura_maquina, velocidade)
                            consumo_diesel = calcular_consumo_diesel(area)
                            emissao_co2 = calcular_emissao_co2(consumo_diesel)
                            custo_diesel = calcular_custo_diesel(consumo_diesel)

                            print("\n----- ESTIMATIVAS -----")
                            print(f"Perdas estimadas: {perdas:.2f}%")
                            print(f"Tempo estimado de colheita: {tempo_colheita:.2f} horas")
                            print(f"Consumo estimado de diesel: {consumo_diesel:.2f} litros")
                            print(f"Emissão estimada de CO2: {emissao_co2:.2f} kg")
                            print(f"Custo estimado de diesel: R$ {custo_diesel:.2f}")

                        case 6:
                            print("----- CONSULTAR OPERAÇÃO DE COLHEITA -----\n")
                            op_id = int(input(margem + "ID da operação a ser consultada: "))

                            cursor.execute(f"SELECT * FROM COLHEITA_CANA WHERE id = {op_id}")
                            operacao = connection.fetchone()

                            if operacao:
                                print(f"ID: {operacao[0]}")
                                print(f"Área: {operacao[1]} hectares")
                                print(f"Velocidade: {operacao[2]} km/h")
                                print(f"Rotação: {operacao[3]}")
                                print(f"Horas sem Afiação: {operacao[4]}")
                                print(f"Colheitadeira: {operacao[5]}")
                                print(f"Largura da Máquina: {operacao[6]} m")
                                print(f"Perdas Estimadas: {operacao[7]:.2f}%")
                                print(f"Tempo Estimado: {operacao[8]:.2f} horas")
                                print(f"Consumo de Diesel: {operacao[9]:.2f} litros")
                                print(f"Emissão de CO2: {operacao[10]:.2f} kg")
                                print(f"Custo de Diesel: R$ {operacao[11]:.2f}")
                                print(f"Data de Início: {operacao[12].strftime('%d/%m/%Y')}")
                                print(f"Data de Fim Estimada: {operacao[13].strftime('%d/%m/%Y')}")
                            else:
                                print("Operação não encontrada.")

                        case 7:
                            break

                        case _:
                            input(margem + "Digite um número entre 1 e 7.")

                    input(margem + "Pressione ENTER para continuar")

    except Exception as exc:
        print("Erro de conexão:", str(exc))
        raise
