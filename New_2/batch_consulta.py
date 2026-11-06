import streamlit as st

# Este app foi desativado em 06/11/2026.
# A consulta de CNPJ em lote agora fica dentro do XClass (acesso restrito):
# https://xclass.pricetax.com.br/cnpj-lote
# O código original está na tag "streamlit-original" deste repositório.

st.set_page_config(page_title="Consulta de CNPJ em Lote - PriceTax", layout="centered")

st.markdown("""
<style>
    .stApp { background-color: #1A1A1A; color: #EEEEEE; }
    h1, h2, h3 { color: #FFC300; }
    a { color: #FFC300 !important; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

st.title("Consulta de CNPJ em Lote")
st.markdown(
    "Esta ferramenta mudou de endereço e agora faz parte do **XClass**, a plataforma da PRICETAX.\n\n"
    "Acesse: [xclass.pricetax.com.br/cnpj-lote](https://xclass.pricetax.com.br/cnpj-lote)\n\n"
    "O acesso é liberado para usuários autorizados. Para solicitar, fale com o seu contato na PRICETAX."
)
