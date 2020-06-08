package com.paic.embeded;

import java.net.URL;

interface InstallationSource {
    String determineVersion();

    URL resolveDownloadUrl();
}

