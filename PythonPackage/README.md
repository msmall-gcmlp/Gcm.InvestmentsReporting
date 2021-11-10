# gcm-investmentsreporting

Investments Team report generation

# Installation

Run the following to install this package

```python
pip install gcm-investmentsreporting
```

# Usage
```python
from gcm.investmentsreporting import say_hello

#Generate "Hello, World!"
say_hello()

# Generate "Hello, Everybody!"
say_hello("Everybody")
```

# Developing Hello World

To install helloworld, along with the tools you need to develop and run tests, run the following in your virutalenv:

```powershell
pip install -e .[dev]
```

```powershell
pip install -e .
tox
python setup.py bdist_wheel sdist
```