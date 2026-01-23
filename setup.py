from setuptools import setup, find_packages

setup(
    name='jupyter-cs3-client',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'jupyter-server',
    ],
    # Declare the Jupyter Server extension
    entry_points={
        'jupyter_server.extension': [
            'cs3_jupyter_client.sharing_extension = cs3_jupyter_client.sharing_extension',
        ],
    },
)