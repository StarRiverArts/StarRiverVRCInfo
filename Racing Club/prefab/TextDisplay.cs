using System.Collections;
using UnityEngine;
using UnityEngine.Networking;
using UnityEngine.UI;

public class TextDisplay : MonoBehaviour
{
    [Tooltip("URL of the summary text file")]
    public string url;

    [Tooltip("UI Text component to display the contents")]
    public Text targetText;

    private void Start()
    {
        if (!string.IsNullOrEmpty(url) && targetText != null)
        {
            StartCoroutine(DownloadText());
        }
    }

    private IEnumerator DownloadText()
    {
        using (UnityWebRequest request = UnityWebRequest.Get(url))
        {
            yield return request.SendWebRequest();
            if (request.result == UnityWebRequest.Result.Success)
            {
                targetText.text = request.downloadHandler.text;
            }
            else
            {
                targetText.text = "Failed to load";
            }
        }
    }
}
