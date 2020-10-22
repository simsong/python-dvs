DVS Web Interface
=================

The DVS web interface uses the following building blocks:



Web Components for DVS Search
-----------------------------

We use the following Custom elements, as explained here:
* https://developers.google.com/web/fundamentals/web-components/customelements
* https://developer.mozilla.org/en-US/docs/Web/API/Window/customElements

We may also use React:
* https://developer.mozilla.org/en-US/docs/Web/Web_Components

### `x-dvs-hash`
This is the basic custom element for displaying hashes in the DVS user interface. It is called like this:

```
<x-dvs-hash alg='md5' value='f1e0961a881c8e15e4e77587255caf5b' more='base64encoded-URL'/>
```
Where:

* `x-dvs-hash` the custom element tag.
* `alg='md5'` the algorithm of the hash
* `value='...'` the hexademical encoding of the hash
* `more='base64encoded-URL'` a Base64-encoded URL that, when decoded, is a link that can be followed for more information about this hash.

Several JavaScript functions in the `dvs_search.js` file make this custom element work:

`dvs_hash_setup()` --- adds the `x-dvs-hash` web component
`dvs_hash_connectedCallback()` --- Called when a new x-dvs-hash element is created (for example, when the document is first loaded). Does the initial setup of the elements that are used to render hashes (e.g. the dots after the hash, the tooltip, etc) and calls `dvs_hash_changedCallback()` to render the initial values.
`dvs_hash_changedCallback()` --- Called to re-render it.


