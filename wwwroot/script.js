var llamadas = 0

document.addEventListener("DOMContentLoaded", function() {
    const button = document.getElementById("myButton");
    button.addEventListener("click", async function() {

        try{
            const respuesta = await fetch('http://localhost:8080/', {
                method: 'POST',
                body: 'el usuario clickeo el boton que indica "Haz click aqui"'
            })

            if(respuesta.ok){
                alert("¡Hola! Has hecho clic en el botón.");
            }
        }
        catch(error){
            window.alert('Error en el boton')
        }
        
    });
});

async function enviarFormulario() {

    const nombre = document.getElementById('nombre').value
    const edad = document.getElementById('edad').value

    try{

    const respuesta = await fetch('http://localhost:8080/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({nombre, edad})
    })

    if(respuesta.ok){
        let texto = document.getElementById('textoAMostrar1')
        texto.innerHTML = `Respuesta: ${respuesta.statusText}`
    }

    }
    catch(error){
        window.alert('Error en envio de formulario')
    }
    
}

async function multiplesSolicitudes(){
    const cantidad = parseInt(document.getElementById('cantidadS').value)
    const nombre = document.getElementById('nombreMultiple').value

    const texto = document.getElementById('textoAMostrar3');

    if(isNaN(cantidad) || cantidad < 1) {
        window.alert('Ingresa una cantidad correcta')
        return;
    }

    const promesas = []

    for(let i = 1; i <= cantidad; i++){
        promesas.push(fetch(`http://localhost:8080/?n=${i}`, {
            method: 'POST',
            body: `${nombre} y numero${i}`
        }))
    }

    try{

        const respuestas = await Promise.all(promesas)
        const ok = respuestas.filter(r => r.ok).length

        texto.innerHTML = `Se enviaron ${cantidad} solicitudes, exitosas: ${ok}`

    }
    catch(error){
        texto.innerHTML = `Error`
        window.alert('Error al enviar las multiples solicitudes')
    }
}


async function probarMetodo(metodo) {

    try{
    const respuesta = await fetch('http://localhost:8080/', {method: metodo})

    this.llamadas = this.llamadas + 1

    let texto = document.getElementById('textoAMostrar2')

    texto.innerHTML = `Respuesta de estatus: ${respuesta.statusText} cantidad de llamados: ${this.llamadas}`
    }

    catch(error){
        alert('error en esta prueba')
    }
}
