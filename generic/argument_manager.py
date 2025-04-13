import argparse
import logging
import os
import sys


class ArgumentManager:
    def __init__(self, description="Script para API"):

        self.description = description
        self.logger = logging.getLogger("ArgManager")

        # Modo debug se chamado sem argumentos
        self.is_debug = len(sys.argv) <= 1

        # Lista de argumentos
        self.args = []

        # Nome base do script (usado como padrão para alguns valores)
        self.script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]

    def add(self, env_var, help_text, required=False, default=None, arg_type=str, action=None):
        arg_name = env_var.lower().replace('_', '-')

        self.args.append({
            'name': arg_name,
            'env': env_var,
            'help': help_text,
            'required': required,
            'default': default,
            'type': arg_type,
            'action': action
        })

        return self

    def parse(self):
        if self.is_debug:

            class Args:
                pass

            result = Args()

            # Define cada argumento a partir do .env
            for arg in self.args:
                env_value = os.getenv(arg['env'])

                # Verifica se é obrigatório e não está definido
                if arg['required'] and env_value is None:
                    raise ValueError(f"Variável de ambiente obrigatória '{arg['env']}' não definida no .env")

                # Define valor padrão se não encontrado no .env
                if env_value is None:
                    value = arg['default']
                else:
                    # Converte o valor para o tipo certo
                    try:
                        if arg['type'] == int:
                            value = int(env_value)
                        elif arg['type'] == float:
                            value = float(env_value)
                        elif arg['type'] == bool or arg['action'] in ['store_true', 'store_false']:
                            value = env_value.lower() in ('true', 'yes', '1', 't')
                        else:
                            value = env_value
                    except Exception:
                        value = arg['default']
                        self.logger.warning(f"Erro convertendo {arg['env']}. Usando valor padrão.")

                # Atribui o valor à propriedade
                setattr(result, arg['env'], value)

        else:
            # Modo normal - usa linha de comando
            parser = argparse.ArgumentParser(description=self.description)

            # Adiciona cada argumento
            for arg in self.args:
                kwargs = {
                    'dest': arg['env'],
                    'help': arg['help']
                }

                # Adiciona propriedades quando presentes
                if arg['required']:
                    kwargs['required'] = True

                if arg['default'] is not None and not arg['action']:
                    kwargs['default'] = arg['default']

                if arg['type'] is not None and not arg['action']:
                    kwargs['type'] = arg['type']

                if arg['action']:
                    kwargs['action'] = arg['action']

                parser.add_argument(f"--{arg['name']}", **kwargs)

            # Parse argumentos
            result = parser.parse_args()

        # Aplica valores especiais
        for arg in self.args:
            # Se for folder_path e estiver vazio, usa o nome do script
            if arg['name'] == 'folder-path' and not getattr(result, arg['env'], None):
                setattr(result, arg['env'], self.script_name)

        return result
